#!/bin/false

# PODNAME: App::Repositorio::Logger
# ABSTRACT: Logging quasi-singleton for Repositorio

use strict;
use warnings;

package App::Repositorio::Logger;

# VERSION

my $logger;

sub load {
    die "Already loaded!\n" if $logger;
    my $package = shift;
    $logger = shift;
    return 1
}

sub new {
    return $logger;
}

1;

__END__

=pod

=encoding utf8

=head1 SYNOPSIS

In bin/yourapp.pl

 use App::Repositorio::Logger;
 # do stuff
 App::Repositorio::Logger->load($logobject);

Then in your lib/YourApp/Base.pm

 use Moo;
 use App::Repositorio::Logger;

 has 'logger' => (
     default => sub { App::Repositorio::Logger->new() },
     );

=head1 METHODS

=head2 load($obj)

Saves $obj for later

=head2 new()

Returns $obj every time
