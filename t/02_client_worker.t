#!perl

use strict;
use warnings;
use Test::Base;
use Test::TCP;
use Test::Deep;

use AnyEvent::Gearman::Client;
use AnyEvent::Gearman::Worker;

eval q{
        use Gearman::Worker;
        use Gearman::Server;
    };
if ($@) {
    plan skip_all
        => "Gearman::Worker and Gearman::Server are required to run this test";
}

# for the purpose of testing
package AnyEvent::Gearman::Types {
    use Any::Moose;
    use Any::Moose '::Util::TypeConstraints';

    subtype 'My::AnyEvent::Gearman::Client::Connections'
	=> as 'ArrayRef[AnyEvent::Gearman::Client::Connection]';

    subtype 'My::AnyEvent::Gearman::Client::StrConnections'
	=> as 'ArrayRef[Str]';

    coerce 'My::AnyEvent::Gearman::Client::Connections'
	=> from 'My::AnyEvent::Gearman::Client::StrConnections' => via {
	    for my $con (@$_) {
		next if ref($con) and $con->isa('My::AnyEvent::Gearman::Client::Connection');
		$con = My::AnyEvent::Gearman::Client::Connection->new( hostspec => $con );
	    }
	    $_;
    };

    no Any::Moose;
}


package My::AnyEvent::Gearman::Client::Connection {
    use Any::Moose;
    extends 'AnyEvent::Gearman::Client::Connection';

    has number_of_tasks_managed => (
	is => 'rw',
	isa => 'Int',
	default => 0,
    );

    no Any::Moose;
    
    sub add_task {
        my $self = shift;
        $self->number_of_tasks_managed++;
	return $self->SUPER::add_task(@_);
    }
}

package My::AnyEvent::Gearman::Client {
    use Any::Moose;
    extends 'AnyEvent::Gearman::Client';

    has job_servers => (
	is       => 'rw',
	isa      => 'My::AnyEvent::Gearman::Client::Connections',
	required => 1,
	coerce   => 1,
    );

    no Any::Moose;
}



plan 'no_plan';

my $port1 = empty_port;
my $port2 = empty_port;
my $port3 = empty_port;
my $port4 = empty_port;
my $port5 = empty_port;

my $number_of_jobs = 5;

sub run_tests {
    my $server_hostspec1 = '127.0.0.1:' . $port1;
    my $server_hostspec2 = '127.0.0.1:' . $port2;
    my $server_hostspec3 = '127.0.0.1:' . $port3;
    my $server_hostspec4 = '127.0.0.1:' . $port4;
    my $server_hostspec5 = '127.0.0.1:' . $port5;

    my $client = My::AnyEvent::Gearman::Client->new(
        job_servers => [$server_hostspec1,
                        $server_hostspec2,
	                $server_hostspec3,
	                $server_hostspec4,
	                $server_hostspec5],
    );

    my $worker = AnyEvent::Gearman::Worker->new(
        job_servers => [$server_hostspec1,
                        $server_hostspec2,
	                $server_hostspec3,
	                $server_hostspec4,
	                $server_hostspec5],
    );

    $worker->register_function( reverse => sub {
        my $job = shift;
        my $res = reverse $job->workload;
        $job->complete($res);
    });
    for (1..$number_of_jobs) {
	      my $cv = AnyEvent->condvar;
	      my $task = $client->add_task(
	          reverse => 'Hello!',
	          on_complete => sub {
	              $cv->send($_[1]);
	          },
	          on_fail => sub {
	              $cv->send('fail');
	          },
	          on_created => sub {
	              my ($task) = @_;
	              my $job_handle = $task->job_handle;
	              ok($job_handle, "Got JOB_CREATED message, got job_handle '$job_handle'");
	          }
	      );
	      ok(!$task->job_handle, 'No job_handle yet');
	      is $cv->recv, reverse('Hello!'), 'reverse ok';
    }

    # this section is a probabilistic test showing that given a set function
    # and workload the same job server is selected
    my @job_server_connections =  @{ $client->job_servers };
    my $job_sum = 0;
    foreach my $js (@job_server_connections) {
	my $jobs = $js->number_of_tasks_managed;
        ok($jobs == 0 || $jobs == $number_of_jobs, 
	   "correct number of jobs: ". $jobs);
	$job_sum += $jobs;
    }

    ok($number_of_jobs == $job_sum, "all jobs accounted for ". $job_sum);
   
    ## Make sure context is sane
    $_->context && is($_->context, $worker) for @{$worker->job_servers};
    $_->context && is($_->context, $client) for @{$client->job_servers};

    # Test bg jobs
    my $cv = AnyEvent->condvar;
    $worker->register_function( bg_done => sub {
        my $job = shift;
        my $work = $job->workload;
        
        $job->complete($work);
        $cv->send("bg job done: $work");
    });
    
    my %cbs;
    $client->add_task_bg(
        bg_done => 'pick me!',
        
        on_created  => sub { $cbs{on_created}++  },
        on_data     => sub { $cbs{on_data}++     },
        on_status   => sub { $cbs{on_status}++   },
        on_warning  => sub { $cbs{on_warning}++  },
        on_complete => sub { $cbs{on_complete}++ },
        on_fail     => sub { $cbs{on_fail}++     },
    );
    
    is $cv->recv, 'bg job done: pick me!';
    cmp_deeply(\%cbs, { on_created => 1 }, 'proper set of callbacks executed');
}

package My::Gearman::Server {
    use base  'Gearman::Server';
    use fields qw( number_of_tasks_managed );

    sub new {
	my ($class, %opts) = @_;
	my $self = fields::new($class);
	$self->SUPER::new(%opts);                # init base fields
	$self->{number_of_tasks_managed} = 0;    # init own fields
	return $self;
    }
    sub add_task  {
	my $self = shift;
	$self->{number_of_tasks_managed}++;
	return self->SUPER::add_task(@_);
    }
}

my $child = fork;
if (!defined $child) {
    die "fork failed: $!";
}
elsif ($child == 0) {
    my @job_servers = (
    my $server1 = My::Gearman::Server->new( port => $port1 ),
    my $server2 = My::Gearman::Server->new( port => $port2 ),
    my $server3 = My::Gearman::Server->new( port => $port3 ),
    my $server4 = My::Gearman::Server->new( port => $port4 ),
    my $server5 = My::Gearman::Server->new( port => $port5 ),
	);
    Danga::Socket->EventLoop;
    exit;
}
else {
    END { kill 9, $child if $child }
}

sleep 1;

run_tests;

