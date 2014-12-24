#!/usr/bin/env perl

use strict;
use warnings;

use Test::More;
use Test::Exception;
use String::Random qw( random_string );

use Parallel::ForkManager::Batch;

use Capture::Tiny qw( tee );
use POSIX qw( floor );
use Readonly;

Readonly my $MAX_PROCS  => 5;
Readonly my $BATCH_SIZE => 20;

subtest 'One Record Batch' => sub {
    my $batch_records = generate_records( 1 );
    test_batch( $batch_records );
};

subtest 'Enough Records Only For 1 Proc' => sub {
    my $batch_records = generate_records( $BATCH_SIZE );
    test_batch( $batch_records );
};

subtest 'Enough Records Only for Half the Procs' => sub {
    my $num_records  = $BATCH_SIZE * ( floor( $MAX_PROCS / 2 ) );
    my $batch_records = generate_records( $num_records );
    test_batch( $batch_records );
};

subtest 'Exact Match - One Round' => sub {
    my $batch_records = generate_records( $MAX_PROCS * $BATCH_SIZE );
    test_batch( $batch_records );
};

subtest 'One and a Half Rounds' => sub {
    my $num_records   = floor( ( $MAX_PROCS * $BATCH_SIZE ) * 1.5 );
    my $batch_records = generate_records( $num_records );
    test_batch( $batch_records );
};

subtest 'Three Rounds' => sub {
    my $batch_records = generate_records( $MAX_PROCS * $BATCH_SIZE * 3);
    test_batch( $batch_records );
};

done_testing;

sub do_it {
    my $record = shift;

    print STDOUT generate_line( $record ) . "\n";
}

sub generate_line {
    my $record = shift;

    return "ID: " . $record->{id} . " - Data: " . $record->{data};
}

sub generate_records {
    my $num_records_to_generate = shift;

    my @batch_records;

    for my $record_number ( 1 .. $num_records_to_generate ) {
        my $record = {
            id   => $record_number,
            data => random_string('cccnnncccnnncccnnn'),
        };

        push @batch_records, $record;
    };

    return \@batch_records;
}

sub test_batch {
    my $batch_records = shift;

    note( 'Batch Has ' . scalar @{ $batch_records } . ' records...' );
    my $pfb = Parallel::ForkManager::Batch->new(
        $MAX_PROCS, $BATCH_SIZE, $batch_records, \&do_it
    );

    my $stdout;
    lives_ok {
        ( $stdout ) = tee { $pfb->run };
    } 'Lives through running of batch';

    my @output = split( "\n", $stdout );

    subtest 'Check Output' => sub {
        for my $record ( @{ $batch_records } ) {
            if( grep { $_ eq generate_line( $record ) } @output ) {
                pass( 'Record ' . $record->{id} . ' was Processed.' );
            }
            else {
                fail( 'Record ' . $record->{id} . ' was not Processed.' );
            }
        }
    };

    return;
}
