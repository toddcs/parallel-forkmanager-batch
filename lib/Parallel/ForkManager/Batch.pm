package Parallel::ForkManager::Batch;

use strict;
use warnings;

use Parallel::ForkManager;

# VERSION: 0.1.0

# ABSTRACT: Calls Parallel::ForkManager on batches of tasks

sub new {
    my ( $class, $max_procs, $batch_size, $batch_records, $doit ) = @_;

    my $self = {};

    $self->{max_procs}     = $max_procs;
    $self->{batch_size}    = $batch_size;
    $self->{batch_records} = $batch_records;
    $self->{doit}          = $doit;

    return bless $self, $class;
}

sub run {
    my $self = shift;

    my $pfm = Parallel::ForkManager->new( $self->{max_procs} );

    my $num_records = scalar @{ $self->{batch_records} };
    my $batch_size  = $self->{batch_size};

    for ( my $i = 0 ; $i < $num_records ; $i += $batch_size ) {
        my $pid = $pfm->start and next;

        for ( my $j = $i ; $j < $i + $batch_size ; ++$j ) {
            last if $j >= $num_records;
            $self->{doit}->( $self->{batch_records}[$j] );
        }

        $pfm->finish;
    }

    $pfm->wait_all_children;

    return;
}

=head1 NAME

    Parallel::ForkManager::Batch

=head1 SYNOPSIS

    my $MAX_PROCESSES = ... ;          # Passed thru to Parallel::ForkManager
    my $BATCH_SIZE    = ... ;          # Max number of tasks per forked process

    my $tasks_to_batch = [
                    # ...
                ];

    sub do_one_task {
        # ... process one batch task
    }

    my $batch = Parallel::ForkManager::Batch->new( $MAX_PROCESSES, $BATCH_SIZE, $tasks_to_batch, \&do_one_task );

    $batch->run;

=head1 DESCRIPTION

Parallel::ForkManager::Batch runs batches of processing using
Parallel::ForkManager as its forking engine. But rather than forking a
dedicated process for each individual task, as Parallel::ForkManager
would normally do, this module arranges for the forking manager to
create only one process for each batch of tasks.

Each batch consists of a predefined number of tasks (except that
the final batch might be smaller).

The current module provides a simple interface for such batching of
tasks within processes.

=head1 METHODS

=head2 new()

    $batch = Parallel::ForkManager::Batch->new( $max_processes, $batch_size, $tasks_to_batch, \&do_one_task );

Calling C<new()> creates and returns a runnable batch object, according to the specs provided by its
arguments. These are:

=over 4

C<$max_processes>

=over 4

The maximum number of processes we want to exist at any given time. This argument is passed thru to
Parallel::ForkManager , who manages this constraint.

=back

C<$batch_size>

=over 4

The maximum number of tasks that will be batched together for processing by one process created by
Parallel::ForkManager .

=back

C<$tasks_to_batch>

=over 4

A reference to an array containing tasks to be batched.

=back

C<\&do_one_task>

=over 4

A code reference, pointing to a subroutine that will be called repeatedly
to process batches of tasks.  For each batch, this subroutine will
be called $BATCH_SIZE times (except for the final batch, which may be
smaller). This module arranges that each time the subroutine is called,
it will receive exactly one argument, a single task from the batch list.

=back

=back

=cut

=head2 run()

    $batch->run()

This method performs all the "heavy lifting" of combining individual tasks
into batches and passing the batches to Parallel::ForkManager . This includes
C<start()>ing each batch in the parallel fork manager, and waiting for all batches to complete.
This method takes no arguments and returns nothing.

=head1 COPYRIGHT

This module is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=head1 AUTHOR

Todd Shandelman (C<< todd.shandelman@gmail.com >>)

Robert Stone (C<< drzigman@cpan.org >>)

=cut

1;
