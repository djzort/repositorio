package App::Repositorio::Plugin::Yum;

# PODNAME: App::Repositorio::Plugin::Yum
# ABSTRACT: Yum handling for Repositorio

use Moo;
use strictures 2;

use Carp;
use IO::Zlib;
use File::Copy qw(copy);
use File::Find qw(find);
use File::Basename qw(basename dirname);
use Params::Validate qw(:all);
use Data::Dumper qw(Dumper);
use XML::Twig;

# VERSION

with 'App::Repositorio::Plugin::Base';

has packages_dir => ( is => 'ro', default => 'Packages' );
has repodata_dir => ( is => 'ro', default => 'repodata' );

sub get_metadata {
  my $self = shift;
  my $arch = shift;

  my $base_dir = File::Spec->catdir($self->dir(), $arch);
  my $packages;

  my @metadata_files = ({type => 'repomd', location => 'repodata/repomd.xml'});
  for my $m (@metadata_files) {
    my $type      = $m->{'type'};
    my $location  = $m->{'location'};
    my $m_url     = join('/', ($self->url, $location));
    $m_url        =~ s/%ARCH%/$arch/;
    my $dest_file = File::Spec->catfile($base_dir, $location);
    my $dest_dir  = dirname($dest_file);

    # Make sure dir exists
    $self->make_dir($dest_dir);

    # Grab the file
    $self->download_binary_file(url => $m_url, dest => $dest_file);

    # Parse the xml and retrieve the primary file location
    if ($type eq 'repomd') {
      my $data = $self->parse_repomd($dest_file);
      push @metadata_files, @{$data};
    }

    # Parse the primary metadata file
    if ($type eq 'primary') {
      my $contents = $self->get_gzip_contents($dest_file);
      $packages = $self->parse_primary($contents);
    }
  }
  return $packages;
}

sub read_metadata {
  my $self = shift;
  my $arch = shift;

  my $base_dir = File::Spec->catdir($self->dir(), $arch);
  my $files = {};

  my @metadata_files = ({type => 'repomd', location => 'repodata/repomd.xml'});
  for my $m (@metadata_files) {
    my $type      = $m->{'type'};
    my $location  = $m->{'location'};
    my $dest_file = File::Spec->catfile($base_dir, $location);
    my $dest_dir  = dirname($dest_file);

    $files->{$location}++;

    if (-f $dest_file) {
      # Parse the xml and retrieve the primary file location
      if ($type eq 'repomd') {
        my $data = $self->parse_repomd($dest_file);
        push @metadata_files, @{$data};
      }

      # Parse the primary metadata file
      if ($type eq 'primary') {
        my $contents = $self->get_gzip_contents($dest_file);
        for my $file (@{$self->parse_primary($contents)}) {
          $files->{$file->{'location'}}++;
        }
      }
    }
  }
  return $files;
}

sub parse_repomd {
  my $self = shift;
  my $file = shift;

  my $twig = XML::Twig->new(TwigRoots => {data => 1});
  $twig->parsefile($file);

  my $root = $twig->root;
  my @e = $root->children();
  my @files;
  for my $e (@e) {
    my $data = {};
    $data->{'type'} = $e->att('type');
    for my $c ($e->children()) {
      if ($c->name eq 'location') {
        $data->{'location'} = $c->att('href');
      }
      elsif ($c->name eq 'checksum') {
        $data->{'checksum'} = $c->att('type');
      }
      elsif ($c->name eq 'size'){
        $data->{'size'} = $c->text;
      }
    }
    $self->logger->log_and_croak(level => 'error', message => "repomd xml not valid: $file") unless $data->{'location'};
    push @files, $data;
  }

  return \@files;
}
sub parse_primary {
  my $self = shift;
  my $xml  = shift;

  my $packages = [];
  my $twig = XML::Twig->new(TwigRoots => {package => 1});
  $twig->parse($xml);
  my $root = $twig->root;
  my @e = $root->children();
  for my $e (@e) {
    my $data = {};
    for my $c ($e->children()) {
      if ($c->name eq 'location') {
        $data->{'location'} = $c->att('href');
      }
      elsif ($c->name eq 'name') {
        $data->{'name'} = $c->text;
      }
      elsif ($c->name eq 'checksum') {
        $data->{'checksum'}->{'type'} = $c->att('type');
        $data->{'checksum'}->{'value'} = $c->text;
      }
      elsif ($c->name eq 'size'){
        $data->{'size'} = $c->att('package');
      }
    }
    push @{$packages}, $data;
  }
  return $packages;
}
sub get_packages {
  my $self = shift;
  my %o = validate(@_, {
    arch     => { type => SCALAR },
    packages => { type => ARRAYREF },
  });

  my $arch     = $o{'arch'};
  my $base_dir = File::Spec->catdir($self->dir(), $arch);

  for my $package (@{$o{'packages'}}) {
    my $name     = $package->{'name'};
    my $size     = $package->{'size'};
    my $location = $package->{'location'};
    my $checksum = $package->{'checksum'};

    my $p_url     = join('/', ($self->url, $location));
    $p_url        =~ s/%ARCH%/$arch/;
    my $dest_file = File::Spec->catfile($base_dir, $location);
    my $dest_dir  = dirname($dest_file);

    # Make sure dir exists
    $self->make_dir($dest_dir);

    # Check if we have the local file
    my $download;
    if ($self->force) {
      $download++;
    }
    elsif ($self->checksums) {
      $download++ unless $self->validate_file(filename => $dest_file, check => $checksum->{'type'}, value => $checksum->{'value'});
    }
    else {
      $download++ unless $self->validate_file(filename => $dest_file, check => 'size', value => $size);
    }

    # Grab the file
    if ($download) {
      $self->logger->notice(sprintf('get_packages; repo: %s arch: %s package: %s', $self->repo(), $arch, $name));
      $self->download_binary_file(url => $p_url, dest => $dest_file);
    }
    else {
      $self->logger->debug(sprintf('get_packages; repo: %s arch: %s package: %s skipping as its deemed up to date', $self->repo(), $arch, $name));
    }
  }
}
sub clean_files {
  my $self = shift;
  my %o = validate(@_, {
    arch  => { type => SCALAR },
    files => { type => HASHREF },
  });

  my $arch     = $o{'arch'};
  my $base_dir = File::Spec->catdir($self->dir(), $arch);

  find(
    sub {
      if (
        $_ !~ /^[\.]+$/
      ) {
        my $file = $_;
        if (-f $file) {
          my $rel = File::Spec->abs2rel($File::Find::name, $base_dir);
          unless ($o{'files'}->{$rel}) {
            $self->logger->info("clean_files; removing non referenced file: ${File::Find::name}");
            unlink $file or $self->logger->log_and_croak(level => 'error', message => "Failed to remove file: ${file}: $!");
          }
        }
      }
    },
    $base_dir,
  );
}

sub add_file {
  my $self  = shift;
  my $arch  = shift;
  my $files = shift;

  unless ($self->validate_arch($arch)) {
    $self->logger->log_and_croak(level => 'error', message => sprintf 'add_file: arch: %s is not in config for repo: %s', $arch, $self->repo());
  }

  for my $file (@${files}) {
    my $filename = basename($file);
    my $dest_file = File::Spec->catfile($self->dir(), $arch, $self->packages_dir(), $filename);
    $self->logger->debug(sprintf 'add_file; repo: %s arch: %s file: %s dest_file: %s', $self->repo(), $arch, $file, $dest_file);

    if (-f $dest_file && ! $self->force()) {
      $self->logger->log_and_croak(level => 'error', message => sprintf 'add_file; repo: %s dest_file exists and force not enabled: %s', $self->repo(), $dest_file);
    }

    copy ($file, $dest_file) || $self->logger->log_and_croak(level => 'error', message => sprintf 'add_file; repo: %s failed to copy file to destination: %s', $self->repo(), $!);

  }

  $self->init_arch($arch);

}

sub del_file {
  my $self  = shift;
  my $arch  = shift;
  my $files = shift;

  unless ($self->validate_arch($arch)) {
    $self->logger->log_and_croak(level => 'error', message => sprintf 'del_file; arch: %s is not in config for repo: %s', $arch, $self->repo());
  }

  for my $file (@${files}) {
    my $filename = basename($file);
    my $dest_file = File::Spec->catfile($self->dir(), $arch, $self->packages_dir(), $filename);
    $self->logger->debug(sprintf 'del_file; repo: %s arch: %s dest_file: %s', $self->repo(), $arch, $dest_file);

    unless (-f $dest_file) {
      $self->logger->log_and_croak(level => 'error', message => sprintf 'del_file; repo: %s dest_file: %s does not exist', $self->repo(), $dest_file);
    }

    unlink $dest_file || $self->logger->log_and_croak(level => 'error', message => sprintf 'del_file; repo: %s failed to del file: %s from destination: %s', $self->repo(), $dest_file, $!);

  }

  $self->init_arch($arch);
}

sub init_arch {
  my $self = shift;
  my $arch = shift;

  my $dir = File::Spec->catdir($self->dir, $arch);

  $self->logger->debug(sprintf 'init_arch; repo: %s arch: %s dir: %s', $self->repo(), $arch, $dir);

  $self->make_dir($dir);
  $self->make_dir(File::Spec->catdir($dir, $self->packages_dir()));

  #XXX add gpg

  #TODO perhaps replace createrepo with pure perl version at some stage
  my $createrepo_bin = $self->find_command_path('createrepo');

  unless ($createrepo_bin and -x $createrepo_bin) {
    $self->logger->log_and_croak(
      level   => 'error',
      message => sprintf('init_arch; repo: %s arch: %s unable to find createrepo program in path', $self->repo(), $arch),
    );
  }

  my @cmd = ($createrepo_bin, '--basedir', $dir, '--outputdir', $dir, $self->packages_dir());
  # --update will reuse the existing metadata if the file is already defined and size/mtime matches
  # dont do this if we're forcing or the repomd.xml doesnt exist
  unless ($self->force()) {
    if (-f File::Spec->catfile($dir, $self->repodata_dir(), 'repomd.xml')) {
      splice @cmd, 1, 0, '--update';
    }
  }

  $self->logger->debug(sprintf 'init_arch; running command: %s', join(' ', @cmd));
  unless (system(@cmd) == 0) {
    $self->logger->log_and_croak(
      level   => 'error',
      message => sprintf(
        'init_arch; repo: %s failed to run command: %s with exit code: %s',
        $self->repo(),
        join(' ', @cmd),
        $?,
      )
    );
  }


}

sub type {
  #my $self = shift;
  return 'Yum';
}

1;
