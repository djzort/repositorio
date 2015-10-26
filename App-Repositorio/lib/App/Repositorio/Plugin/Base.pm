package App::Repositorio::Plugin::Base;

# PODNAME: App::Repositorio::Plugin::Base
# ABSTRACT: Base class for all package handling in Repositorio

use Moo::Role;
use strictures 2;
use namespace::clean;

use Carp;
use Data::Dumper;
use Digest::SHA;
use File::Find qw(find);
use File::Path qw(make_path remove_tree);
use File::Spec;
use LWP::UserAgent;
use Module::Path qw[ module_path ];
use Module::Runtime qw[ compose_module_name ];
use Params::Validate qw(:all);
use Time::HiRes qw(gettimeofday tv_interval);

# VERSION

has logger    => ( is => 'ro', required => 1 );
has repo      => ( is => 'ro', required => 1 );
has dir       => ( is => 'ro', required => 1 );
has url       => ( is => 'ro', optional => 1 );
has checksums => ( is => 'ro', optional => 1 );
has force     => ( is => 'ro', optional => 1 );
has arches    => ( is => 'ro', required => 1 );
has ua        => ( is => 'lazy' );
has ssl_ca    => ( is => 'ro', optional => 1 );
has ssl_cert  => ( is => 'ro', optional => 1 );
has ssl_key   => ( is => 'ro', optional => 1 );

sub _build_ua {
  my $self = shift;

  my %o;
  $o{ssl_opts}->{'SSL_ca_file'}   = $self->ssl_ca()   if $self->can('ssl_ca');
  $o{ssl_opts}->{'SSL_cert_file'} = $self->ssl_cert() if $self->can('ssl_cert');
  $o{ssl_opts}->{'SSL_key_file'}  = $self->ssl_key()  if $self->can('ssl_key');

  return LWP::UserAgent->new(%o);
}

sub get_gzip_contents {
  my $self = shift;
  my $file = shift;

  if (-f $file) {
    {
        my $fh = IO::Zlib->new($file, 'rb');

        #XXX for some reason this does not work
        #local $/ = undef;
        #my $contents = <$fh>;
        #return $contents;

        my @contents = <$fh>;
        $fh->close;
        return join('', @contents);
    }
  }
}

sub find_command_path {
  my $self    = shift;
  my $command = shift || return;

  my @path = File::Spec->path();
  for my $p (@path) {
    my $command_path = File::Spec->catfile($p, $command);
    return $command_path if -f $command_path;
  }
  return;
}

sub make_dir {
  my $self = shift;
  my $dir  = shift;
  if (! -d $dir) {
    my $dirs = make_path($dir);
    $self->logger->log_and_croak(level => 'error', message => "Failed to create path: ${dir}") unless -d $dir;
    $self->logger->debug("Created path: ${dir}");
    return 1;
  }
  return 0;
}
sub remove_dir {
  my $self = shift;
  my $dir  = shift;
  if (-d $dir) {
    my $dirs = remove_tree($dir);
    $self->logger->log_and_croak(level => 'error', message => "Failed to remove path: ${dir}") if -d $dir;
    $self->logger->debug("removed path: ${dir}");
    return 1;
  }
  return 0;
}

sub validate_arch {
  my $self = shift;
  my $arch = shift;

  my $matched;
  for my $a (@{$self->arches()}) {
    $matched++ if $a eq $arch;
  }

  return $matched;
}
sub validate_file {
  my $self = shift;
  my %o = validate(@_, {
    filename => { type => SCALAR },
    check    => { type => SCALAR },
    value    => { type => SCALAR },
  });

  return 0 unless -f $o{'filename'};

  if ($o{'check'} eq 'size') {
    return $self->_validate_file_size($o{'filename'}, $o{'value'});
  }
  elsif ($o{'check'} eq 'sha256') {
    return $self->_validate_file_sha256($o{'filename'}, $o{'value'});
  }
}

sub _validate_file_size {
  my $self = shift;
  my $file = shift;
  my $size = shift;

  my @stats     = stat($file);
  my $file_size = $stats[7];

  return $file_size eq $size ? 1 : undef;
}

sub _validate_file_sha256 {
  my $self     = shift;
  my $file     = shift;
  my $checksum = shift;

  my $sha = Digest::SHA->new('sha256');
  $sha->addfile($file);
  return $sha->hexdigest eq $checksum ? 1 : undef;
}

sub mirror {
  my $self = shift;

  for my $arch (@{$self->arches()}) {
    $self->logger->info(sprintf("mirror: starting repo: %s arch: %s from url: %s to dir: %s", $self->repo, $arch, $self->url, $self->dir));
    my $packages = $self->get_metadata($arch);
    $self->get_packages(arch => $arch, packages => $packages);
  }

}
sub clean {
  my $self = shift;

  $self->logger->info(sprintf("clean: starting repo: %s in dir: %s", $self->repo, $self->dir));
  for my $arch (@{$self->arches()}) {
    my $files = $self->read_metadata($arch);
    $self->clean_files(arch => $arch, files => $files);
  }

}

sub init {
  my $self = shift;
  my $arch = shift;
  $self->logger->info(sprintf 'init: repo: %s dir: %s', $self->repo(), $self->dir());
  if ($arch) {
    $self->init_arch($arch);
  }
  else {
    for my $a (@{$self->arches()}) {
      $self->init_arch($a);
    }
  }

}

sub tag {
  my $self = shift;
  my %o = validate(@_, {
    src_tag  => { type => SCALAR },
    src_dir  => { type => SCALAR },
    dest_tag => { type => SCALAR },
    dest_dir => { type => SCALAR },
    symlink  => { type => BOOLEAN, default => 0 },
  });

  $self->logger->debug(sprintf('tag: repo: %s tagging: %s -> %s', $self->repo(), $o{'src_dir'}, $o{'dest_dir'}));

  # When src_dir does not exist do not continue
  $self->logger->log_and_die(
    level   => 'error',
    message => sprintf("tag: repo: %s src_dir: %s does not exist", $self->repo(), $o{'src_dir'}),
  ) unless -d $o{'src_dir'};

  # When dest_dir exists and force is not set do not continue
  # Handle symbolic link destinations
  if ( -l $o{'dest_dir'} ) {
    if ($self->force()) {
      unlink $o{'dest_dir'};
    }
    else {
      $self->logger->log_and_die(
        level   => 'error',
        message => sprintf("tag: repo: %s dest_dir: %s exists and force not enabled.", $self->repo(), $o{'dest_dir'}),
      );
    }
  }
  # Handle hard linked destinations
  elsif ( -d $o{'dest_dir'} ) {
    if ($self->force() ) {
      $self->remove_dir($o{'dest_dir'});
    }
    else {
      $self->logger->log_and_die(
        level   => 'error',
        message => sprintf("tag: repo: %s dest_dir: %s exists and force not enabled.", $self->repo(), $o{'dest_dir'}),
      );
    }
  }

  # Setup the new destination

  # handle hardlinked destination
  unless ($o{'symlink'}) {
    $self->make_dir($o{'dest_dir'});
    $self->logger->debug(sprintf('tag: repo: %s hardlink src_dir: %s dest_dir: %s', $self->repo(), $o{'src_dir'}, $o{'dest_dir'}));
    find(
      sub {
        if (
          $_ !~ /^[\.]+$/
        ) {
          my $src_path = $File::Find::name;
          my $path = File::Spec->abs2rel($File::Find::name, $o{'src_dir'});
          if (-d $src_path) {
            $self->make_dir(File::Spec->catdir($o{'dest_dir'}, $path));
          }
          elsif (-f $src_path) {
            my $dest_path = File::Spec->catfile($o{'dest_dir'}, $path);

            if (link $src_path, $dest_path) {
              $self->logger->debug(sprintf('tag: repo: %s hardlink: %s', $self->repo(), $path));
            }
            else {
              $self->logger->log_and_croak(
                level => 'error',
                message => sprintf('tag: repo: %s failed to hardlink: %s to %s',
                  $self->repo(),
                  $src_path,
                  $dest_path,
                ),
              );
            }
          }
        }
      },
      $o{'src_dir'},
    );

  }

  # handle symlink destination
  else {
    if (symlink $o{'src_dir'}, $o{'dest_dir'}) {
      $self->logger->debug(sprintf('tag: repo: %s symlink src_dir: %s dest_dir: %s', $self->repo(), $o{'src_dir'}, $o{'dest_dir'}));
    }
    else {
      $self->logger->log_and_die(
        level   => 'error',
        message => sprintf("tag: repo: %s couldnt link src_dir: %s to dst_dir: %s: $!", $self->repo(), $o{'src_dir'}, $o{'dest_dir'}),
      );
    }
  }
}

sub download_binary_file {
  my $self = shift;
  my %o = validate(@_, {
    url         => { type => SCALAR },
    dest        => { type => SCALAR },
    retry_limit => { type => SCALAR, default => 3 },
  });

  $self->logger->debug(
    sprintf(
      'download_binary_file: repo: %s url: %s dest: %s',
      $self->repo(),
      $o{url},
      $o{dest},
    )
  );

  my $retry_count = 0;
  my $retry_limit = $o{retry_limit};
  my $success;

  while (!$success && $retry_count <= $retry_limit) {
    my $t0 = [gettimeofday];
    my $res = $self->ua->get($o{'url'}, ':content_file' => $o{'dest'});
    my $elapsed = tv_interval($t0);

    $self->logger->debug(
      sprintf(
        'download_binary_file: repo: %s url: %s took: %s',
        $self->repo(),
        $o{url},
        $elapsed,
      )
    );

    if ($res->is_success) {
      return 1;
    }
    else {
      $self->logger->debug(
        sprintf(
          'download_binary_file: repo: %s url: %s failed with status: %s',
          $self->repo(),
          $o{url},
          $res->status_line,
        )
      );
      $retry_count++;
      if ($retry_count <= $retry_limit) {
        $self->logger->debug(
          sprintf(
            'download_binary_file: repo: %s url: %s retrying',
            $self->repo(),
            $o{url},
          )
        ) if $retry_count;
      }
      else {
        $self->logger->log_and_croak(
          level => 'error',
          message => sprintf(
            'download_binary_file: repo: %s url: %s failed and exhausted all retries',
            $self->repo(),
            $o{url},
          )
        );
      }
    }
  }
}

1;
