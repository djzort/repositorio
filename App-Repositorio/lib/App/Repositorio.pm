#!/bin/false
package App::Repositorio;

# PODNAME: App::Repositorio
# ABSTRACT: The core part of repositorio

use Moo;
use strictures 2;
use namespace::clean;
use Carp;
use POSIX qw(strftime);
use Cwd qw(getcwd);
use Params::Validate qw(:all);
use Fcntl qw/:flock LOCK_EX LOCK_UN LOCK_NB/;
use Module::Pluggable::Object;
use File::Spec;
use App::Repositorio::Logger;

# VERSION

has config => ( is => 'ro' );
has logger => (
  is => 'ro',
  lazy => 1,
  default => sub { App::Repositorio::Logger->new() }
);

# these are for convenience
my @repos;
my %check_repo = (
  'repo is configured' => sub {
    my $r = shift;
    my $v = shift;
    return 1 if $v->{regex};
    return 1 if $r eq 'all';
    return ( grep { $r eq $_ } @repos ) ? 1 : 0;
  }
);
my %check_tag = (
  'sensible tag characters' => sub {
    my $r = shift;
    return $r =~ m/^[A-z0-9\-_]+$/;
  }
);

=head1 DESCRIPTION

App::Repositorio - An application to handle management of various software repositories
This module is purely to designed to be used with the accompanying script bin/rpio
Please look at its pod for instantiating of this Module

=head2 SYNOPSIS

  my $o = App::Repositorio->new(
    config => $hashref,
  );

  $o->go($action, %options);

=head2 METHODS

=over 4

=cut

=item B<go()>

Perform the supplied action with options

Valid actions:

=over 4

=item B<add-file>

Used to add file/s to a local repository see  L</"add_file()">

=item B<del-file>

Used to delete file/s from a local repository see L</"del_file()">

=item B<clean>

Used to clean a repository of no longer referenced files eg: no longer reference package versions or manifest files see L</"clean()">

=item B<init>

Used to initialise manifests for a local repository see L<"/init()">

=item B<list>

Used to list configured repositories see L<"/list()">

=item B<mirror>

Used to update a repository from its configured mirror see L<"/mirror()">

=item B<tag>

Tag a repository state see L<"/tag()">

=back

=cut

sub go {
  my ( $self, $action, @args ) = @_;

  $self->logger->info( 'starting: ' . strftime( '%F %T %z', localtime ) );

  $self->_validate_config();

  my $dispatch = {
    'add-file' => \&add_file,
    'del-file' => \&del_file,
    'diff'     => \&diff,
    'clean'    => \&clean,
    'init'     => \&init,
    'list'     => \&list,
    'mirror'   => \&mirror,
    'tag'      => \&tag,
  };

  exists $dispatch->{$action}
    || $self->logger->log_and_croak(
    level   => 'error',
    message => "ERROR: ${action} not supported.\n"
    );

  $dispatch->{$action}->( $self, @args );

  $self->logger->info( 'finished: ' . strftime( '%F %T %z', localtime ) );

  return 1;
}

{

  my $lockfh;
  my $lockf;

  sub _lock {
    my $self = shift;
    my $repo = shift;
    my $dir  = shift;
    croak "Can't lock, directory doesnt exist: $dir\n" unless -d $dir;
    $lockf = File::Spec->catfile( $dir, "$repo.lock" );
    $self->logger->info("Locking $repo via $lockf");
    open( $lockfh, '>', $lockf )
      or croak "Error opening lock file: $!";
    flock( $lockfh, LOCK_EX | LOCK_NB )
      or $self->logger->log_and_croak(
      level   => 'error',
      message => "Couldnt lock $repo"
      );
    return 1;
  }

  sub _unlock {
    my $self = shift;
    my $repo = shift;
    $self->logger->info("Unlocking $repo via $lockf");
    flock( $lockfh, LOCK_EX )
      or $self->logger->log_and_croak(
      level   => 'error',
      message => "Couldnt unlock $repo"
      );
    close $lockfh;
    return 1;
  }

  END {
    if ( defined $lockfh && fileno $lockfh ) {
      flock( $lockfh, LOCK_EX );
      close $lockfh;
    }
    if ( $lockf and -f $lockf ) {
      unlink $lockf;
    }
  }

}

sub _validate_config {
  my $self = shift;

  # If data_dir is relative, lets expand it based on cwd
  $self->config->{'data_dir'} =
    File::Spec->rel2abs( $self->config->{data_dir} );

  # Make sure data_dir exists
  $self->logger->log_and_croak(
    level   => 'error',
    message => sprintf "datadir does not exist: %s\n",
    $self->config->{data_dir},
  ) unless -d $self->config->{data_dir};

  # Ensure tag style option is valid
  $self->logger->log_and_croak(
    level   => 'error',
    message => sprintf "Unknown tag_style %s, must be topdir or bottomdir\n",
    $self->config->{tag_style},
  ) unless $self->config->{tag_style} =~ m/^(?:top|bottom)dir$/;

  # do this once, and keep it hanging around as useful sideeffect
  @repos = sort keys %{ $self->config->{'repo'} };

  # required params for each repo config
  for my $repo (@repos) {

    # Unfortunately Config::General does not allow us to make sure an option is always an array, so force it to an array
    if ( my $url = $self->config->{'repo'}->{$repo}->{'url'} ) {
      my $urles = ref($url) eq 'ARRAY' ? $url : [$url];
      $self->config->{'repo'}->{$repo}->{'url'} = $urles;

      # FIXME plugins should be able to check their config, rather than doing it here
      my @missing;
      for my $param (qw/ca cert key/) {
        if (my $file = $self->config->{repo}->{$repo}->{$param}) {
          $self->logger->log_and_croak(
            level   => 'error',
            message => sprintf "repo: %s param: %s value: %s error: not a file\n",
            $repo, $param, $file
          ) unless -f $file;
        }
        else {
          push @missing, $param;
        }
      }

      $self->logger->log_and_croak(
        level   => 'error',
        message => sprintf "repo: %s missing param: %s\n",
        $repo, join(',', @missing)
      ) if (@missing > 0 and @missing < 3 );

      my @filter;
      for my $param (qw/include_filename include_package exclude_filename exclude_package/) {
        push @filter, $param if $self->config->{repo}->{$repo}->{$param};
      }

      $self->logger->log_and_croak(
        level   => 'error',
        message => sprintf "repo: %s param: %s error: only one allowed\n",
        $repo, join(',', @filter)
      ) if (@filter > 1)

    }
    else {

      $self->logger->log_and_croak(
        level   => 'error',
        message => sprintf "repo: %s error: ca, cert and key only valid for url repos\n",
        $repo
      ) if (grep {$_ =~ m/^(ca|cert|key)$/ } keys %{$self->config->{'repo'}->{$repo}})

    } # if my $url

    # If there is a global proxy setting, set it in the repo UNLESS the repo has its own proxy
    if ($self->config->{'proxy'}) {
      $self->config->{'repo'}->{$repo}->{'proxy'} ||= $self->config->{'proxy'}
    }

    # type local and arch are required params for ALL repos
  REPO_PARAM_LOOP:
    for my $param (qw/type local arch/) {
      $self->logger->log_and_croak(
        level   => 'error',
        message => sprintf "repo: %s missing param: %s\n",
        $repo, $param,
      ) unless $self->config->{'repo'}->{$repo}->{$param};

      # Data validation for specific types

      # Unfortunately Config::General does not allow us to make sure an option is always an array, so force it to an array
      if ( $param eq 'arch' ) {
        # We allow identical options which we use for arch, lets end up with an array regardless
        my $arch = $self->config->{'repo'}->{$repo}->{'arch'};
        my $arches = ref($arch) eq 'ARRAY' ? $arch : [$arch];
        $self->config->{'repo'}->{$repo}->{'arch'} = $arches;
        next REPO_PARAM_LOOP;
      } # if ( $param eq 'arch' ) {

      # Allowed types
      if ( $param eq 'type' ) {
        unless ( # FIXME this should come from the loaded plugins
          $self->config->{repo}->{$repo}->{$param} eq 'Yum'
          || $self->config->{repo}->{$repo}->{$param} eq 'Apt'
          || $self->config->{repo}->{$repo}->{$param} eq 'Plain',
          )
        {
          $self->logger->log_and_croak(
            level   => 'error',
            message => sprintf
              "repo; %s param: %s value: %s is not supported\n",
            $repo, $param,
            $self->config->{repo}->{$repo}->{$param},
          );
        }
        next REPO_PARAM_LOOP;
      } # if ( $param eq 'type' ) {
    } # REPO_PARAM_LOOP
  }

  return 1;
}

sub _get_plugin {
  my $self = shift;
  my %o    = validate_with(
    params => \@_,
    spec   => {
      type    => { type    => SCALAR, },
      options => { options => HASHREF, },
    }
  );

  my $plugin;
  for my $p (
    Module::Pluggable::Object->new(
      instantiate => 'new',
      search_path => ['App::Repositorio::Plugin'],
      except      => ['App::Repositorio::Plugin::Base'],
    )->plugins( %{ $o{'options'} } )
    )
  {
    $plugin = $p if $p->type() eq $o{'type'};
  }
  $self->logger->log_and_croak(
    level   => 'error',
    message => "Failed to find a plugin for type: $o{'type'}\n"
  ) unless $plugin;
  return $plugin;
}

sub _get_repo_dir {
  my $self = shift;
  my %o    = validate_with(
    params => \@_,
    spec   => {
      repo => { type => SCALAR },
      tag  => { type => SCALAR, default => 'head', },
    }
  );

  my $data_dir  = $self->config->{data_dir};
  my $tag_style = $self->config->{tag_style};
  my $repo      = $o{'repo'};
  my $tag       = $o{'tag'};
  my $local     = $self->config->{'repo'}->{$repo}->{'local'};

  if ( $tag_style eq 'topdir' ) {
    return File::Spec->catdir( $data_dir, $tag, $local );
  }
  elsif ( $tag_style eq 'bottomdir' ) {
    return File::Spec->catdir( $data_dir, $local, $tag );
  }
  else {
    $self->logger->log_and_croak(
      level   => 'error',
      message => '_get_repo_dir: Unknown tag_style: ' . $tag_style . "\n"
    );
  }
}

=item B<add_file()>

Action: add-file

Description: Adds a file to a local repository and updates the related metadata

Options:

=over 4

=item repo

The name of the repository as reflected in the config

=item arch

The arch this package should be added to as reflected in the config

=item file

The path of the file to be added to the repository

=item force

Boolean to enable force overwriting an existing file in the repository

=back

=cut

sub add_file {
  my $self = shift;
  my %o    = validate_with(
    params => @_,
    spec   => {
      'repo'  => { type => SCALAR,  callbacks => \%check_repo },
      'arch'  => { type => SCALAR },
      'file'  => { type => SCALAR | ARRAYREF },
      'force' => { type => BOOLEAN, default   => 0 },
    },
  );
  my $options = {
    repo    => $o{'repo'},
    arches  => $self->config->{'repo'}->{ $o{'repo'} }->{'arch'},
    dir     => $self->_get_repo_dir( repo => $o{'repo'} ),
    force   => $o{'force'},
  };
  my $plugin = $self->_get_plugin(
    type    => $self->config->{'repo'}->{ $o{'repo'} }->{'type'},
    options => $options,
  );

  $plugin->add_file( $o{'arch'}, $o{'file'} );
}

=item B<del_file()>

Action: del-file

Description: Removes a file to a local repository and updates the related metadata

Options:

=over 4

=item repo

The name of the repository as reflected in the config

=item arch

The arch this package should be removed from as reflected in the config

=item file

The filename to be removed to the repository

=back

=cut

sub del_file {
  my $self = shift;
  my %o    = validate_with(
    params => \@_,
    spec   => {
      'repo' => { type => SCALAR, callbacks => \%check_repo },
      'arch' => { type => SCALAR },
      'file' => { type => SCALAR | ARRAYREF },
    },
  );
  my $options = {
    repo    => $o{'repo'},
    arches  => $self->config->{'repo'}->{ $o{'repo'} }->{'arch'},
    dir     => $self->_get_repo_dir( repo => $o{'repo'} ),
    force   => $o{'force'},
  };
  my $plugin = $self->_get_plugin(
    type    => $self->config->{'repo'}->{ $o{'repo'} }->{'type'},
    options => $options,
  );

  $plugin->make_dir( $options->{dir} ) unless -d $options->{dir};
  $self->_lock( $options->{repo}, $options->{dir} );
  $plugin->del_file( $o{'arch'}, $o{'file'} );
  $self->_unlock( $options->{repo} );
}

=item B<diff()>

Action: diff

Description: Displays the difference between two tags in the same repo

Options:

=over 4

=item repo

The name of the repository as reflected in the config.

=item arch

The arch that should be examined.

=item src-tag

The source tag to use for this operation, by default this is 'head'
The source tag must pre exist.

=item dest-tag

The destination tag to use for this operation.
The source tag must pre exist.

=back

=cut

sub diff {
  my $self = shift;
  my %o    = validate_with(
    params => \@_,
    spec   => {
      'repo'    => { type => SCALAR, callbacks => \%check_repo },
      'tag'     => { type => SCALAR },
      'src-tag' => { type => SCALAR, default => 'head' },
      'arch'    => { type => SCALAR },
    },
  );
  my $options = {
    repo    => $o{'repo'},
    arches  => $self->config->{'repo'}->{ $o{'repo'} }->{'arch'},
    dir     => $self->_get_repo_dir( repo => $o{'repo'} ),
  };
  my $plugin = $self->_get_plugin(
    type    => $self->config->{'repo'}->{ $o{'repo'} }->{'type'},
    options => $options,
  );

  my $result = $plugin->diff(
    src_dir  => $self->_get_repo_dir( repo => $o{'repo'}, tag => $o{'src-tag'} ),
    src_tag  => $o{'src-tag'},
    dest_dir => $self->_get_repo_dir( repo => $o{'repo'}, tag => $o{'tag'} ),
    dest_tag => $o{'tag'},
    arch     => $o{'arch'}
  );

  printf "|%20s|%20s|\n", $o{'src-tag'}, $o{'tag'};
  my %files = (
              (map {+($_ => 'src-tag')} @{$result->{$o{'src-tag'}}}),
              (map {+($_ => 'dest-tag')} @{$result->{$o{'tag'}}})
              );

  for my $r (sort keys %files) {
    printf "|%20s|%20s|\n",
         ($files{$r} eq 'src-tag' ? $r : ''),
         ($files{$r} eq 'dest-tag' ? $r : '')
  }

}

=item B<clean()>

Action: clean

Description: Removes files from a repository that are not referenced in the metadata

Options:

=over 4

=item repo

The name of the repository as reflected in the config
If 'all' is supplied it will perform this action on all repositories in config

=item regex

If this boolean is enabled then use the repo parameter as a regex to match repositories against

=back

=cut

sub clean {
  my $self = shift;
  my %o    = validate_with(
    params => \@_,
    spec   => {
      repo  => { type => SCALAR,  callbacks => \%check_repo },
      arch  => { type => SCALAR,  optional  => 1 },
      regex => { type => BOOLEAN, optional  => 1 },
      force => { type => BOOLEAN, optional  => 1, },
    }
  );

  # treat the 'repo' value as regex
  if ( $o{'regex'} ) {
    my %options = %o;
    my $regex   = qr#$o{'repo'}#;
    for my $repo (@repos) {
      if ( $repo =~ $regex ) {
        $options{'repo'} = $repo;
        $self->_clean(%options);
      }
    }
    return 1;
  }

  # handle the 'all' special case
  if ( $o{'repo'} eq 'all' ) {
    my %options = %o;
    for my $repo (@repos) {
      $options{'repo'} = $repo;
      $self->_clean(%options);
    }
    return 1;
  }

  # otherwise, do this
  $self->_clean(%o);
}

sub _clean {
  my ( $self, %o ) = @_;

  my $options = {
    repo    => $o{'repo'},
    arches  => $self->config->{'repo'}->{ $o{'repo'} }->{'arch'},
    dir     => $self->_get_repo_dir( repo => $o{'repo'} ),
    force   => $o{'force'},
  };
  my $plugin = $self->_get_plugin(
    type    => $self->config->{'repo'}->{ $o{'repo'} }->{'type'},
    options => $options,
  );

  $plugin->make_dir( $options->{dir} ) unless -d $options->{dir};
  $self->_lock( $options->{repo}, $options->{dir} );
  $plugin->clean();
  $self->_unlock( $options->{repo} );
}

=item B<init()>

Action: init

Description: Initialises a custom repository by generating the appropriate metadata files

Options:

=over 4

=item repo

The name of the repository as reflected in the config

=item arch

Rather than initialising all arches configured, just do this one

=back

=cut

sub init {
  my $self = shift;
  my %o    = validate_with(
    params => \@_,
    spec   => {
      repo => { type => SCALAR, callbacks => \%check_repo },
      arch => { type => SCALAR, optional  => 1 },
    }
  );

  my $repo_config = $self->config->{'repo'}->{ $o{'repo'} };

# FIXME plugins themselves should decide if they can be init'd
# Initialising a mirrored repo will result in different manifests to what the mirror has
  if ( $repo_config->{'url'} ) {
    $self->logger->log_and_croak(
      level => 'error',
      message =>
        'init: this is action is only valid for local repositories...this repo has a url specified',
    );
  }

  my $options = {
    repo    => $o{'repo'},
    arches  => $repo_config->{'arch'},
    dir     => $self->_get_repo_dir( repo => $o{'repo'} ),
  };

  my $plugin = $self->_get_plugin(
    type    => $repo_config->{'type'},
    options => $options,
  );

  $plugin->make_dir( $options->{dir} ) unless -d $options->{dir};
  $self->_lock( $options->{repo}, $options->{dir} );
  $plugin->init( $o{'arch'} );
  $self->_lock( $options->{repo}, $options->{dir} );
}

=item B<list()>

Action: list

Description: Lists the repositories as reflected in the config, or lists
current tags if a repo name is provided.

Options:

=over 4

=item repo

The repo, from which the tags will be listed.

=item format

The output format. Either I<json>, I<csv> or I<default>.

=back

=cut

sub _list_repos {
  my $self = shift;
  my %o    = @_;

  # perhaps this next section could be more elegant, but doing things manually keeps deps down

  if ( $o{format} eq 'default' ) {
    print "Repository list:\n";
    printf "|%8s|%8s|%50s|\n", 'Type', 'Mirrored', 'Name';
    for my $repo (@repos) {
      my $type = $self->config->{repo}->{$repo}->{type};
      my $mirrored = $self->config->{repo}->{$repo}->{url} ? 'Yes' : 'No';
      print sprintf "|%8s|%8s|%50s|\n", $type, $mirrored, $repo;
    }
    return 1;
  }

  if ( $o{format} eq 'csv' ) {
    print join( ',', 'Type', 'Mirrored', 'Name' );
    for my $repo (@repos) {
      if ( $repo =~ m/[,"]/ ) {
        $repo =~ s/"/\\"/g;
        $repo = qq|"$repo"|;
      }
      my $type = $self->config->{repo}->{$repo}->{type};
      my $mirrored = $self->config->{repo}->{$repo}->{url} ? 'Yes' : 'No';
      print "\n", join( ',', $type, $mirrored, $repo );
    }
    return 1;
  }

  if ( $o{format} eq 'json' ) {
    my @list;
    for my $repo (@repos) {
      $repo =~ s/"/\\"/g;
      my $type = $self->config->{repo}->{$repo}->{type};
      my $mirrored = $self->config->{repo}->{$repo}->{url} ? 'true' : 'false';
      push @list, qq|{"type":"$type","mirrored":$mirrored,"name":"$repo"}|;
    }
    print '{"repos":[', join( ',', @list ), "]}\n";
    return 1;
  }

  # shouldnt get here
  die 'unknown format? shouldnt get here';

}

# FIXME this should probably be implemened in the plugin?
sub _list_tags {

  my $self = shift;
  my %o    = @_;

  my %tags;
  my $path;
  {
    my $data_dir  = $self->config->{data_dir};
    my $tag_style = $self->config->{tag_style};
    my $repo      = $o{'repo'};
    my $tag       = $o{'tag'};
    my $local     = $self->config->{'repo'}->{$repo}->{'local'};

    if ( $tag_style eq 'topdir' ) {
      $path = $data_dir
    }
    elsif ( $tag_style eq 'bottomdir' ) {
      $path = File::Spec->catdir( $data_dir, $local );
    }
    else {
      $self->logger->log_and_croak(
        level   => 'error',
        message => '_list_tags; Unknown tag_style: ' . $tag_style . "\n"
      );
    }
  }

  opendir(my $dh, $path)
    or $self->logger->log_and_croak(
         level => 'error',
         message => '_list_tags; Unable to opendir: ' . $!
      );

  for my $tag ( grep { $_ !~ m/^\.\.?$/ }
                readdir($dh)) {

    my $tagpath = File::Spec->catdir( $path, $tag );
    if ( -d $tagpath ) {
      if ( -l $tagpath ) {
        my $htag = readlink $tagpath;
        $htag = (File::Spec->splitdir($htag))[-1];
        push @{$tags{$htag}}, $tag;
      }
      else {
        $tags{$tag} ||= [];
      }
    }
  }

  close $dh;

  if ( $o{format} eq 'default' ) {
    print "Tag list for $o{repo}:\n";
    printf "|%20s|%20s|\n", 'Name', 'Soft Tag';
    for my $tag (sort keys %tags) {
      print sprintf "|%20s|%20s|\n", $tag, '';
      for my $stag (@{$tags{$tag}}) {
        print sprintf "|%20s|%20s|\n", $tag, $stag;
      }
    }
    return 1;
  }

  if ( $o{format} eq 'json' ) {
    my @list;
    for my $tag (sort keys %tags) {
      my $line = '{"tag":"' . $tag . '"';
      if (@{$tags{$tag}}) {
        $line .= ',"soft tag":[';
        $line .= join(',',map { qq|"$_"| } @{$tags{$tag}});
        $line .= ']';
      };
      $line .= '}';
      push @list, $line
    }
    print '{"repo":"',$o{'repo'},'",';
    print '"tags":[', join( ',', @list );
    print "]}\n";
    return 1;
  }

}

sub list {
  my $self = shift;
  my %o    = validate_with(
    params => \@_,
    spec   => {
      repo   => {
        type => SCALAR,
        optional => 1,
        callbacks => \%check_repo
      },
      format => {
        type    => SCALAR,
        default => 'default',
        regex   => qw/^(default|json|csv)$/
      },
    }
  );

  if ($o{repo}) {
    return $self->_list_tags(%o)
  }

  return $self->_list_repos(%o)

}

=item B<mirror()>

Action: mirror

Description: Mirrors repository from upstream provider into the head tag

Options:

=over 4

=item repo

The name of the repository as reflected in the config
If 'all' is supplied it will perform this action on all repositories in config

=item checksums

By default we just use the manifests information about size of packages to determine if the local file
is valid. If you want to have checksums used enable this boolean flag.
With this enabled updating a mirror can take quite a long time

=item regex

If this boolean is enabled then use the repo parameter as a regex to match repositories against

=back

=cut

sub mirror {
  my $self = shift;
  my %o    = validate_with(
    params => \@_,
    spec   => {
      'repo'          => { type => SCALAR,  callbacks => \%check_repo },
      'force'         => { type => BOOLEAN, default   => 0 },
      'ignore-errors' => { type => BOOLEAN, default   => 0 },
      'arch'          => { type => SCALAR,  optional  => 1 },
      'checksums'     => { type => SCALAR,  optional  => 1 },
      'regex'         => { type => BOOLEAN, optional  => 1 },
    }
  );

  # treat the 'repo' value as regex
  if ( $o{'regex'} ) {
    my %options = %o;
    my $regex   = qr#$o{'repo'}#;
    for my $repo (@repos) {
      next unless $repo =~ $regex;
      $options{'repo'} = $repo;
      $self->_mirror(%options);
    }
    return 1;
  }

  # handle the 'all' special case
  if ( $o{'repo'} eq 'all' ) {
    my %options = %o;
    for my $repo (@repos) {
      $options{'repo'} = $repo;
      $self->_mirror(%options);
    }
    return 1;
  }

  # otherwise, do this
  $self->_mirror(%o);
}

sub _mirror {
  my ( $self, %o ) = @_;

  my $options = {
    repo      => $o{'repo'},
    arches    => $self->config->{'repo'}->{ $o{'repo'} }->{'arch'},
    url       => $self->config->{'repo'}->{ $o{'repo'} }->{'url'},
    checksums => $o{'checksums'},
    dir       => $self->_get_repo_dir( repo => $o{'repo'} ),
    include_filename => $self->config->{'repo'}->{ $o{'repo'} }->{'include_filename'} || undef,
    include_package => $self->config->{'repo'}->{ $o{'repo'} }->{'include_package'} || undef,
    exclude_filename => $self->config->{'repo'}->{ $o{'repo'} }->{'exclude_filename'} || undef,
    exclude_package => $self->config->{'repo'}->{ $o{'repo'} }->{'exclude_package'} || undef,
    ssl_ca    => $self->config->{'repo'}->{ $o{'repo'} }->{'ca'} || undef,
    ssl_cert  => $self->config->{'repo'}->{ $o{'repo'} }->{'cert'} || undef,
    ssl_key   => $self->config->{'repo'}->{ $o{'repo'} }->{'key'} || undef,
    force     => $o{'force'},
    'ignore_errors' => $o{'ignore-errors'},
  };
  my $plugin = $self->_get_plugin(
    type    => $self->config->{'repo'}->{ $o{'repo'} }->{'type'},
    options => $options
  );
  $plugin->make_dir( $options->{dir} ) unless -d $options->{dir};
  $self->_lock( $options->{repo}, $options->{dir} );
  $plugin->mirror();
  $self->_unlock( $options->{repo} );
}

=item B<tag()>

Action: tag

Description: Tags a repository at a particular state

Options:

=over 4

=item repo

The name of the repository as reflected in the config

=item src-tag

The source tag to use for this operation, by default this is 'head'
The source tag must pre exist.

=item dest-tag

The destination tag to use for this operation.

=item symlink

This will make the link operation use a symlink instead of hardlinking
For example you may tag every time you update from upstream but you move a production tag around...provides easy roll back
for your clients package configuration

=item force

Force will overwrite a pre existing dest-tag location

=back

=cut

sub tag {
  my $self = shift;
  my %o    = validate_with(
    params => \@_,
    spec   => {
      'repo'    => { type => SCALAR,  callbacks => \%check_repo },
      'tag'     => { type => SCALAR,  callbacks => \%check_tag },
      'src-tag' => { type => SCALAR,  default   => 'head' },
      'symlink' => { type => BOOLEAN, default   => 0 },
      'force'   => { type => BOOLEAN, default   => 0 },
    },
  );

  my $options = {
    repo    => $o{'repo'},
    arches  => $self->config->{'repo'}->{ $o{'repo'} }->{'arch'},
    dir     => $self->_get_repo_dir( repo => $o{'repo'} ),
    force   => $o{'force'},
  };

  my $plugin = $self->_get_plugin(
    type    => $self->config->{'repo'}->{ $o{'repo'} }->{'type'},
    options => $options,
  );

  $plugin->make_dir( $options->{dir} ) unless -d $options->{dir};
  $self->_lock( $options->{repo}, $options->{dir} );
  $plugin->tag(
    src_dir  => $self->_get_repo_dir( repo => $o{'repo'}, tag => $o{'src-tag'} ),
    src_tag  => $o{'src-tag'},
    dest_dir => $self->_get_repo_dir( repo => $o{'repo'}, tag => $o{'tag'} ),
    dest_tag => $o{'tag'},
    symlink  => $o{'symlink'},
    hard_tag_regex => $self->config->{'repo'}->{'hard_tag_regex'}
      || $self->config->{'hard_tag_regex'},
  );
  $self->_unlock( $options->{repo} );
}

1;

=back

=cut

__END__

# vim: softtabstop=2 tabstop=2 shiftwidth=2 ft=perl expandtab smarttab
