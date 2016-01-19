#!/bin/false
package App::Repositorio::Plugin::Yum;

# PODNAME: App::Repositorio::Plugin::Yum
# ABSTRACT: Yum handling for Repositorio

use Moo;
use strictures 2;

use Carp;
use File::Copy qw(copy);
use File::Find qw(find);
use File::Basename qw(basename dirname);
use IO::Zlib;
use Params::Validate qw(:all);
use Time::HiRes qw(gettimeofday tv_interval);
use XML::Twig;
use XML::LibXML;

# VERSION

with 'App::Repositorio::Plugin::Base';

has packages_dir => ( is => 'ro', default => 'Packages' );
has repodata_dir => ( is => 'ro', default => 'repodata' );

my @metadata_files_base =
  ( { type => 'repomd', location => 'repodata/repomd.xml' } );

sub get_metadata {
  my $self = shift;
  my $arch = shift;

  my $base_dir = File::Spec->catdir( $self->dir(), $arch );
  my $packages;

URL_LOOP:
  for my $url ( @{ $self->{url} } ) {

    my @metadata_files = @metadata_files_base;
    for my $m (@metadata_files) {
      my $type     = $m->{'type'};
      my $location = $m->{'location'};
      my $m_url    = join( '/', $url, $location );
      $m_url =~ s/%ARCH%/\Q$arch\E/g;
      my $dest_file = File::Spec->catfile( $base_dir, $location );
      my $dest_dir = dirname($dest_file);

      # Make sure dir exists
      $self->make_dir($dest_dir);

      # Check if we have the local file
      my $download;
      if ( $type eq 'repomd' ) {
        $download++;
      }
      elsif (
        !$self->validate_file(
          filename => $dest_file,
          check    => $m->{'validate'}->{'type'},
          value    => $m->{'validate'}->{'value'},
        )
        )
      {
        $download++;
      }

      # Grab the file
      if ($download) {
        eval {
          $self->download_binary_file( url => $m_url, dest => $dest_file );
        };
        if ($@) {
          next URL_LOOP unless $self->ok_url;
          if ( $self->ignore_errors ) {
            $self->logger->debug( %{$@} );
            return;
          }
          $self->logger->log_and_croak( %{$@} );
        }
        $self->ok_url($url) unless $self->ok_url;
      }
      else {
        $self->logger->debug(
          sprintf(
            'get_metadata; repo: %s arch: %s file: %s skipping as its deemed up to date',
            $self->repo(), $arch, $location
          )
        );
      }

      # Parse the xml and retrieve the primary file location
      if ( $type eq 'repomd' ) {
        my $data = $self->parse_repomd($dest_file);
        push @metadata_files, @{$data};
      }

      # Parse the primary metadata file
      if ( $type eq 'primary' ) {
        $packages = $self->parse_primary($dest_file);
      }
    }
    last URL_LOOP if $self->ok_url;
  }
  return $packages;
}

sub read_metadata {
  my $self = shift;
  my $arch = shift;

  my $base_dir = File::Spec->catdir( $self->dir, $arch );

  return $self->_read_metadata($base_dir)

}

sub _read_metadata {

  my $self = shift;
  my $base_dir = shift;
  my $files = {};

  my @metadata_files = @metadata_files_base;
  for my $m (@metadata_files) {
    my $type      = $m->{'type'};
    my $location  = $m->{'location'};
    my $dest_file = File::Spec->catfile( $base_dir, $location );
    my $dest_dir  = dirname($dest_file);

    $files->{$location}++;

    if ( -f $dest_file ) {

      # Parse the xml and retrieve the primary file location
      if ( $type eq 'repomd' ) {

        $self->logger->debug(
          sprintf(
            'read_metadata; repo: %s file: %s reading as repomod file',
            $self->repo(), $dest_file
          )
        );

        my $data = $self->parse_repomd($dest_file);
        push @metadata_files, @{$data};
      }

      # Parse the primary metadata file
      if ( $type eq 'primary' ) {

        $self->logger->debug(
          sprintf(
            'read_metadata; repo: %s file: %s reading as primary file',
            $self->repo(), $dest_file
          )
        );

        my $contents = $self->get_gzip_contents($dest_file);
        for my $file ( @{ $self->parse_primary($dest_file) } ) {
          $files->{ $file->{'location'} }++;
        }
      }
    }
  }
  return $files;
}

sub parse_repomd {
  my $self = shift;
  my $file = shift;

  # FIXME rework this with XML::LibXML as its far faster
  my $twig = XML::Twig->new( TwigRoots => { data => 1 } );
  $twig->parsefile($file);

  my $root = $twig->root;
  my @e    = $root->children();
  my @files;
  for my $e (@e) {
    my $data = {};
    $data->{'type'} = $e->att('type');
    for my $c ( $e->children() ) {
      if ( $c->name eq 'location' ) {
        $data->{'location'} = $c->att('href');
      }
      elsif ( $c->name eq 'checksum' ) {
        $data->{'checksum'}->{'type'}  = $c->att('type');
        $data->{'checksum'}->{'value'} = $c->text;
      }
      elsif ( $c->name eq 'size' ) {
        $data->{'size'}->{'type'}  = 'size';
        $data->{'size'}->{'value'} = $c->text;
      }
    }

    # For some reason i have found a few repomd.xml files that do NOT
    # have a size attribute ...specifically updateinfo type
    # so as a work around we will try size if checksums is not enabled
    # however for that file we'll revert to checksums if size is not available
    if ( !$self->checksums() && $data->{'size'} ) {
      $data->{'validate'} = $data->{'size'};
    }
    else {
      $data->{'validate'} = $data->{'checksum'};
    }
    $self->logger->log_and_croak(
      level   => 'error',
      message => "repomd xml not valid: $file"
    ) unless $data->{'location'};
    push @files, $data;
  }

  return \@files;
}

sub parse_primary {
  my $self      = shift;
  my $dest_file = shift;
  my $io_fh     = IO::Zlib->new( $dest_file, 'rb' );
  my $xml       = XML::LibXML->load_xml( IO => $io_fh );
  my $t0        = [gettimeofday];
  my $packages  = [];
  for my $p ( $xml->getElementsByTagName('package') ) {
    my ($n)  = $p->getChildrenByTagName('name');
    my ($l)  = $p->getChildrenByTagName('location');
    my ($s)  = $p->getChildrenByTagName('size');
    my ($c)  = $p->getChildrenByTagName('checksum');
    my $data = {
      name     => $n->textContent,
      location => $l->getAttribute('href'),
      size     => {
        type  => 'size',
        value => $s->getAttribute('package'),
      },
      checksum => {
        type  => $c->getAttribute('type'),
        value => $c->textContent,
      },
    };
    if ( !$self->checksums() && $data->{'size'} ) {
      $data->{'validate'} = $data->{'size'};
    }
    else {
      $data->{'validate'} = $data->{'checksum'};
    }
    push @{$packages}, $data;
  }
  my $elapsed = tv_interval($t0);
  $self->logger->debug(
    sprintf( 'parse_primary: file: %s took: %s seconds', $dest_file, $elapsed )
  );
  return [ sort { $a->{name} cmp $b->{name} } @$packages ];
}

sub get_packages {
  my $self = shift;
  my %o    = validate(
    @_,
    {
      arch     => { type => SCALAR },
      packages => { type => ARRAYREF },
    }
  );

  my $count    = 0;
  my $arch     = $o{'arch'};
  my $base_dir = File::Spec->catdir( $self->dir(), $arch );

  for my $package ( @{ $o{'packages'} } ) {
    my $name     = $package->{'name'};
    my $location = $package->{'location'};

    if ($self->filter($package)) {
      $self->logger->debug(
        sprintf(
          'get_packages; repo: %s arch: %s package: %s skipping due to filter',
          $self->repo(), $arch, $location
        )
      );
      next
    }

    my $p_url = join( '/', ( $self->ok_url, $location ) );
    $p_url =~ s/%ARCH%/\Q$arch\E/g;
    my $dest_file = File::Spec->catfile( $base_dir, $location );
    my $dest_dir = dirname($dest_file);

    # Make sure dir exists
    $self->make_dir($dest_dir);

    # Check if we have the local file
    if (
      $self->validate_file(
        filename => $dest_file,
        check    => $package->{'validate'}->{'type'},
        value    => $package->{'validate'}->{'value'},
      )
      )
    {
      $self->logger->debug(
        sprintf(
          'get_packages; repo: %s arch: %s package: %s skipping as its deemed up to date',
          $self->repo(), $arch, $location
        )
      );
      next;
    }

    $self->logger->notice(
      sprintf(
        'get_packages; repo: %s arch: %s package: %s',
        $self->repo(), $arch, $location
      )
    );
    eval {
      $count +=
        $self->download_binary_file( url => $p_url, dest => $dest_file );
    };
    if ($@) {
      $self->logger->log_and_croak( %{$@} ) unless $self->ignore_errors;
      $self->logger->debug( %{$@} );
    }
  }
  return $count;
}

sub clean_files {
  my $self = shift;
  my %o    = validate(
    @_,
    {
      arch  => { type => SCALAR },
      files => { type => HASHREF },
    }
  );

  my $arch = $o{'arch'};
  my $base_dir = File::Spec->catdir( $self->dir(), $arch );

  find(
    sub {
      if ( $_ !~ /^[\.]+$/ ) {
        my $file = $_;
        if ( -f $file ) {
          my $rel = File::Spec->abs2rel( $File::Find::name, $base_dir );
          unless ( $o{'files'}->{$rel} ) {
            $self->logger->info(
              "clean_files; removing non referenced file: ${File::Find::name}");
            unlink $file
              or $self->logger->log_and_croak(
              level   => 'error',
              message => "Failed to remove file: ${file}: $!"
              );
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

  unless ( $self->validate_arch($arch) ) {
    $self->logger->log_and_croak(
      level   => 'error',
      message => sprintf 'add_file; arch: %s is not in config for repo: %s',
      $arch, $self->repo()
    );
  }

  if ( $self->url ) {
    $self->logger->log_and_croak(
      level   => 'error',
      message => sprintf 'add_file; repo: %s is a mirrored repo, cannot add files',
      $self->repo()
    );
  }

  my $package_dir =
    File::Spec->catdir( $self->dir(), $arch, $self->packages_dir() );
  $self->make_dir($package_dir) unless -d $package_dir;

  for my $file ( @${files} ) {

    $self->logger->log_and_croak(
        level   => 'error',
        message => sprintf
          'add_file; repo: %s file: %s error: does not exist',
        $self->repo(), $file
    ) unless -f $file;

    my $filename = basename($file);
    my $dest_file = File::Spec->catfile( $package_dir, $filename );
    $self->logger->debug(
      sprintf 'add_file; repo: %s arch: %s file: %s dest_file: %s',
      $self->repo(), $arch, $file, $dest_file );

    if ( -f $dest_file && !$self->force() ) {
      $self->logger->log_and_croak(
        level   => 'error',
        message => sprintf
          'add_file; repo: %s dest_file exists and force not enabled: %s',
        $self->repo(), $dest_file
      );
    }

    copy( $file, $dest_file ) || $self->logger->log_and_croak(
      level   => 'error',
      message => sprintf
        'add_file; repo: %s failed to copy file to destination: %s',
      $self->repo(), $!
    );

  }

  $self->init_arch($arch);

}

sub del_file {
  my $self  = shift;
  my $arch  = shift;
  my $files = shift;

  unless ( $self->validate_arch($arch) ) {
    $self->logger->log_and_croak(
      level   => 'error',
      message => sprintf 'del_file; arch: %s is not in config for repo: %s',
      $arch, $self->repo()
    );
  }

  for my $file ( @${files} ) {
    my $filename = basename($file);
    my $dest_file =
      File::Spec->catfile( $self->dir(), $arch, $self->packages_dir(),
      $filename );
    $self->logger->debug( sprintf 'del_file; repo: %s arch: %s dest_file: %s',
      $self->repo(), $arch, $dest_file );

    unless ( -f $dest_file ) {
      $self->logger->log_and_croak(
        level   => 'error',
        message => sprintf 'del_file; repo: %s dest_file: %s does not exist',
        $self->repo(), $dest_file
      );
    }

    unlink $dest_file || $self->logger->log_and_croak(
      level   => 'error',
      message => sprintf
        'del_file; repo: %s failed to del file: %s from destination: %s',
      $self->repo(), $dest_file, $!
    );

  }

  $self->init_arch($arch);
}

sub init_arch {
  my $self = shift;
  my $arch = shift;

  my $dir = File::Spec->catdir( $self->dir, $arch );

  $self->logger->debug( sprintf 'init_arch; repo: %s arch: %s dir: %s',
    $self->repo(), $arch, $dir );

  $self->make_dir($dir);
  $self->make_dir( File::Spec->catdir( $dir, $self->packages_dir() ) );

  #FIXME add gpg

  #TODO perhaps replace createrepo with pure perl version at some stage
  my $createrepo_bin = $self->find_command_path('createrepo');

  unless ( $createrepo_bin and -x $createrepo_bin ) {
    $self->logger->log_and_croak(
      level   => 'error',
      message => sprintf(
        'init_arch; repo: %s arch: %s unable to find createrepo program in path',
        $self->repo(), $arch
      ),
    );
  }

  my @cmd = ( $createrepo_bin, '--basedir', $dir, '--outputdir', $dir, $dir );

# --update will reuse the existing metadata if the file is already defined and size/mtime matches
# dont do this if we're forcing or the repomd.xml doesnt exist
  unless ( $self->force() ) {
    if ( -f File::Spec->catfile( $dir, $self->repodata_dir(), 'repomd.xml' ) ) {
      splice @cmd, 1, 0, '--update';
    }
  }

  $self->logger->debug( sprintf 'init_arch; running command: %s',
    join( ' ', @cmd ) );
  unless ( system(@cmd) == 0 ) {
    $self->logger->log_and_croak(
      level   => 'error',
      message => sprintf(
        'init_arch; repo: %s failed to run command: %s with exit code: %s',
        $self->repo(), join( ' ', @cmd ), $?,
      )
    );
  }

}

sub diff {
  my $self = shift;
  my %o    = validate(
    @_,
    {
      arch     => { type => SCALAR },
      src_tag  => { type => SCALAR },
      src_dir  => { type => SCALAR },
      dest_tag => { type => SCALAR },
      dest_dir => { type => SCALAR },
    }
  );

  $self->logger->debug(
    sprintf(
      'diff; repo: %s arch: %s comparing: %s vs %s',
      $self->repo(),$o{'arch'},$o{'src_tag'},$o{'dest_tag'}
    )
  );

  # Check each tag exists
  for my $dir (qw/src_dir dest_dir/) {

    $self->logger->log_and_die(
      level   => 'error',
      message => sprintf(
        'diff; repo: %s %s: %s does not exist',
        $self->repo(), $dir, $o{$dir}
      ),
    ) unless -d $o{$dir};

  }

  my $srcpackages  = $self->_read_metadata(File::Spec->catdir( $o{src_dir}, $o{arch} ));
  my $destpackages = $self->_read_metadata(File::Spec->catdir( $o{dest_dir}, $o{arch} ));

  my %diff;

  for my $file (keys %$srcpackages) {
    $diff{basename($file)}--
  }
  for my $file (keys %$destpackages) {
    $diff{basename($file)}++
  }

  my %result = ( $o{src_tag} => [], $o{dest_tag} => [] );
  for my $k (sort keys %diff) {
    next unless $k =~ m/\.rpm$/; # FIXME this is crude
    push @{$result{$o{src_tag}}}, $k  if $diff{$k} < 0;
    push @{$result{$o{dest_tag}}}, $k if $diff{$k} > 0;
  }

  return \%result

}

sub type {

  #my $self = shift;
  return 'Yum';
}

1;

# vim: softtabstop=2 tabstop=2 shiftwidth=2 ft=perl expandtab smarttab
