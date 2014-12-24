package Parallel::ForkManager::Batch;

use Parallel::ForkManager;

sub new {
    my ( $class, $max_procs, $batch_size, $batch_records, $doit ) = @_;

    my $self = {};

    $self->{max_procs}     = $max_procs;
    $self->{batch_size}    = $batch_size;
    $self->{batch_records} = $batch_records;
    $self->{doit}          = $doit;

    return bless $self;
}

sub run {
    $self = shift;

    my $pfm = new Parallel::ForkManager( $self->{max_procs} );

    $num_records = scalar @{ $self->{batch_records} };
    $batch_size  = $self->{batch_size};

    for ( my $i = 0 ; $i < $num_records ; $i += $batch_size ) {
        my $pid = $pfm->start and next;

        for ( my $j = $i ; $j < $i + $batch_size ; ++$j ) {
            last if $j >= $num_records;
            $self->{doit}->( $self->{batch_records}[$j] );
        }

        $pfm->finish;
    }

    $pfm->wait_all_children;
}

1;
