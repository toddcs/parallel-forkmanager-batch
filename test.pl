#!/usr/bin/env perl

use strict;
use warnings;

use Parallel::ForkManager::Batch;

my $MAX_PROCESSES = 3;
my $BATCH_SIZE    = 8;

srand 9876543210;

my @batch;
for ( 1 .. 50 ) { push @batch, int rand 1_000; }

sub do_one {
    my $record = shift;
    printf "child: record = [[%s]]\n", $record;
}

my $batch = Parallel::ForkManager::Batch->new( $MAX_PROCESSES, $BATCH_SIZE, \@batch, \&do_one );

$batch->run;

