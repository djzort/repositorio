package App::Repositorio;
# PODNAME: App::Repositorio
# ABSTRACT: The core part of repositorio

use Moo;
use strictures 2;
use namespace::clean;
use Carp;
use Cwd qw(getcwd);
use Data::Dumper;
use Params::Validate qw(:all);
use Module::Pluggable::Object;
use File::Spec;

# VERSION


has config => ( is => 'ro' );
has logger => ( is => 'ro' );

sub go {
  my $self = shift;
  my $action = shift;
  my @o = @_;
  my %options;
  $self->_validate_config();

  $self->logger->log_and_croak(level => 'error', message => 'ERROR: action not supplied.') unless $action;

  my %actions;

  $actions{'add-file'} = sub { $self->add_file(@o) };
  $actions{'del-file'} = sub { $self->del_file(@o) };
  $actions{'clean'} = sub {
    %options = validate(
      @o,
      {
        'repo' => { type => SCALAR },
        'arch' => { type => SCALAR, optional => 1 },
      },
    );
    if ($options{'repo'} eq 'all') {
      my %o = %options;
      for my $repo (keys %{$self->config->{'repo'}}) {
        $o{'repo'} = $repo;
        $self->clean(%o);
      }
    }
    else {
      $self->clean(%options);
    }
  };
  $actions{'init'} = sub { $self->init(@o) };
  $actions{'list'} = sub { $self->list() };
  $actions{'mirror'} = sub {
    %options = validate(
      @o,
      {
        'repo'      => { type => SCALAR },
        'force'     => { type => BOOLEAN, default  => 0 },
        'regex'     => { type => BOOLEAN, default  => 0 },
        'arch'      => { type => SCALAR,  optional => 1 },
        'checksums' => { type => SCALAR,  optional => 1},
      },
    );
    # treat the 'repo' as a regex
    if ($options{'regex'}) {
      my %o = %options;
      my $regex = qr/$options{'repo'}/;
      for my $repo (sort keys %{$self->config->{'repo'}}) {
        next unless $repo =~ $regex;
        $o{'repo'} = $repo;
        $self->mirror(%o);
        return 1
      }
    }
    # 'all' is special case
    if ($options{'repo'} eq 'all') {
      my %o = %options;
      for my $repo (sort keys %{$self->config->{'repo'}}) {
        $o{'repo'} = $repo;
        $self->mirror(%o);
        return 1
      }
    }
    # fall back
    return $self->mirror(%options);
  };
  $actions{'tag'} = sub {
    %options = validate(
      @o,
      {
        'repo'    => { type => SCALAR },
        'tag'     => { type => SCALAR },
        'src-tag' => { type => SCALAR,  default => 'head' },
        'symlink' => { type => BOOLEAN, default => 0 },
        'force'   => { type => BOOLEAN, default => 0 },
      },
    );
    $self->tag(%options);
  };

  if ($actions{$action}) {
      $actions{$action}->();
  }
  else {
    $self->logger->log_and_croak(level => 'error', message => "ERROR: ${action} not supported.");
  }
  exit(0);
}
sub _validate_config {
  my $self = shift;

  # If data_dir is relative, lets expand it based on cwd
  $self->config->{'data_dir'} = File::Spec->rel2abs($self->config->{data_dir});

  $self->logger->log_and_croak(
    level   => 'error',
    message => sprintf "datadir does not exist: %s", $self->config->{data_dir},
  ) unless -d $self->config->{data_dir};

  $self->logger->log_and_croak(
    level   => 'error',
    message => sprintf "Unknown tag_style %s, must be topdir or bottomdir\n", $self->config->{tag_style},
  ) unless $self->config->{tag_style} =~ m/^(?:top|bottom)dir$/;

  # required params for reposrc-tag
  for my $repo (sort keys %{$self->config->{'repo'}}) {
    for my $param (qw/type local arch/) {
      $self->logger->log_and_croak(
        level   => 'error',
        message => sprintf "repo: %s missing param: %s", $repo, $param,
      ) unless $self->config->{repo}->{$repo}->{$param};
      # Data validation for specific type
      if ($param eq 'arch') {
        # We allow identical options which we use for arch, lets end up with an array regardless
        my $arch = $self->config->{'repo'}->{$repo}->{'arch'};
        my $arches = ref($arch) eq 'ARRAY' ? $arch : [$arch];
        $self->config->{'repo'}->{$repo}->{'arch'} = $arches;
      }
      elsif ($param eq 'type') {
        unless (
          $self->config->{repo}->{$repo}->{$param} eq 'Yum' ||
          $self->config->{repo}->{$repo}->{$param} eq 'Apt' ||
          $self->config->{repo}->{$repo}->{$param} eq 'Plain',
        ) {
          $self->logger->log_and_croak(
            level   => 'error',
            message => sprintf 'repo; %s param: %s value: %s is not supported', $repo, $param, $self->config->{repo}->{$repo}->{$param},
          );
        }
      }
    }
  }
}

sub _get_plugin {
  my $self    = shift;
  my %o = validate(@_, {
    type      => { type    => SCALAR, },
    options   => { options => HASHREF, },
  });

  my $plugin;
  for my $p (Module::Pluggable::Object->new(
    instantiate => 'new',
    search_path => ['App::Repositorio::Plugin'],
    except      => ['App::Repositorio::Plugin::Base'],
  )->plugins(%{$o{'options'}}) ) {
    $plugin = $p if $p->type() eq $o{'type'};
  }
  $self->logger->log_and_croak(level => 'error', message => "Failed to find a plugin for type: $o{'type'}") unless $plugin;
  return $plugin;
}

sub _get_repo_dir {
  my $self = shift;
  my %o = validate(@_, {
    repo => { type => SCALAR },
    tag =>  { type => SCALAR, default => 'head', },
  });

  my $data_dir  = $self->config->{data_dir};
  my $tag_style = $self->config->{tag_style};
  my $repo      = $o{'repo'};
  my $tag       = $o{'tag'};
  my $local     = $self->config->{'repo'}->{$repo}->{'local'};

  if ($tag_style eq 'topdir') {
    return File::Spec->catdir($data_dir, $tag, $local);
  }
  elsif ($tag_style eq 'bottomdir') {
    return File::Spec->catdir($data_dir, $local, $tag);
  }
  else {
    $self->logger->log_and_croak(level => 'error', message => '_get_repo_dir: Unknown tag_style: '.$tag_style);
  }
}

sub add_file {
  my $self = shift;
  my %o = validate(
    @_,
    {
      'repo'      => { type => SCALAR },
      'arch'      => { type => SCALAR },
      'file'      => { type => SCALAR | ARRAYREF },
      'force'     => { type => BOOLEAN, default => 0 },
    },
  );
  my $options = {
    logger    => $self->logger(),
    repo      => $o{'repo'},
    arches    => $self->config->{'repo'}->{$o{'repo'}}->{'arch'},
    backend   => $self->config->{'repo'}->{$o{'repo'}}->{'type'},
    dir       => $self->_get_repo_dir(repo => $o{'repo'}),
    force     => $o{'force'},
  };
  my $plugin = $self->_get_plugin(
    type    => $self->config->{'repo'}->{$o{'repo'}}->{'type'},
    options => $options,
  );

  $plugin->add_file($o{'arch'}, $o{'file'});
}

sub del_file {
  my $self = shift;
  my %o = validate(
    @_,
    {
      'repo'      => { type => SCALAR },
      'arch'      => { type => SCALAR },
      'file'      => { type => SCALAR | ARRAYREF },
    },
  );
  my $options = {
    logger    => $self->logger(),
    repo      => $o{'repo'},
    arches    => $self->config->{'repo'}->{$o{'repo'}}->{'arch'},
    backend   => $self->config->{'repo'}->{$o{'repo'}}->{'type'},
    dir       => $self->_get_repo_dir(repo => $o{'repo'}),
    force     => $o{'force'},
  };
  my $plugin = $self->_get_plugin(
    type    => $self->config->{'repo'}->{$o{'repo'}}->{'type'},
    options => $options,
  );

  $plugin->del_file($o{'arch'}, $o{'file'});
}

sub clean {
  my $self = shift;
  my %o = validate(@_, {
    repo      => { type => SCALAR, },
    force     => { type => BOOLEAN, optional => 1, },
  });

  my $options = {
    logger    => $self->logger(),
    repo      => $o{'repo'},
    arches    => $self->config->{'repo'}->{$o{'repo'}}->{'arch'},
    backend   => $self->config->{'repo'}->{$o{'repo'}}->{'type'},
    dir       => $self->_get_repo_dir(repo => $o{'repo'}),
    force     => $o{'force'},
  };
  my $plugin = $self->_get_plugin(
    type    => $self->config->{'repo'}->{$o{'repo'}}->{'type'},
    options => $options,
  );

  $plugin->clean();
}

sub init {
  my $self = shift;
  my %o = validate(@_, {
    repo => { type => SCALAR, },
    arch => { type => SCALAR, optional => 1 },
  });

  my $options = {
    logger    => $self->logger(),
    repo      => $o{'repo'},
    arches    => $self->config->{'repo'}->{$o{'repo'}}->{'arch'},
    backend   => $self->config->{'repo'}->{$o{'repo'}}->{'type'},
    dir       => $self->_get_repo_dir(repo => $o{'repo'}),
  };

  my $plugin = $self->_get_plugin(
    type    => $self->config->{'repo'}->{$o{'repo'}}->{'type'},
    options => $options,
  );

  $plugin->init($o{'arch'});
}

sub list {
  my $self = shift;
  print "Repository list:\n";
  print sprintf "|%8s|%8s|%50s|\n", 'Type', 'Mirrored', 'Name';
  for my $repo (sort keys %{$self->config->{repo}}) {
    my $type     = $self->config->{repo}->{$repo}->{type};
    my $mirrored = $self->config->{repo}->{$repo}->{url} ? 'Yes' : 'No';
    print sprintf "|%8s|%8s|%50s|\n", $type, $mirrored, $repo;
  }
}

sub mirror {
  my $self = shift;
  my %o = validate(@_, {
    repo      => { type => SCALAR, },
    checksums => { type => BOOLEAN, optional => 1, },
    force     => { type => BOOLEAN, optional => 1, },
    regex     => { type => BOOLEAN, optional => 1, },
  });

  my $options = {
    logger    => $self->logger(),
    repo      => $o{'repo'},
    arches    => $self->config->{'repo'}->{$o{'repo'}}->{'arch'},
    url       => $self->config->{'repo'}->{$o{'repo'}}->{'url'},
    checksums => $o{'checksums'},
    backend   => $self->config->{'repo'}->{$o{'repo'}}->{'type'},
    dir       => $self->_get_repo_dir(repo => $o{'repo'}),
    ssl_ca    => $self->config->{'repo'}->{$o{'repo'}}->{'ca'} || undef,
    ssl_cert  => $self->config->{'repo'}->{$o{'repo'}}->{'cert'} || undef,
    ssl_key   => $self->config->{'repo'}->{$o{'repo'}}->{'key'} || undef,
    force     => $o{'force'},
  };
  my $plugin = $self->_get_plugin(
    type    => $self->config->{'repo'}->{$o{'repo'}}->{'type'},
    options => $options
  );
  $plugin->mirror();
}

sub tag {
  my $self = shift;
  my %o = @_;
  my $options = {
    logger    => $self->logger(),
    repo      => $o{'repo'},
    arches    => $self->config->{'repo'}->{$o{'repo'}}->{'arch'},
    backend   => $self->config->{'repo'}->{$o{'repo'}}->{'type'},
    dir       => $self->_get_repo_dir(repo => $o{'repo'}),
    force     => $o{'force'},
  };

  my $plugin = $self->_get_plugin(
    type    => $self->config->{'repo'}->{$o{'repo'}}->{'type'},
    options => $options,
  );

  $plugin->tag(
    src_dir  => $self->_get_repo_dir(repo => $o{'repo'}, tag => $o{'src-tag'}),
    src_tag  => $o{'src-tag'},
    dest_dir => $self->_get_repo_dir(repo => $o{'repo'}, tag => $o{'tag'}),
    dest_tag => $o{'tag'},
    symlink  => $o{'symlink'},
  );
}



1;

__END__


