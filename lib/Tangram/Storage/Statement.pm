
package Tangram::Storage::Statement;

sub new
{
    my $class = shift;
    bless { @_ }, $class;
}

sub fetchrow
{
    return shift->{statement}->fetchrow;
}

sub close
{
    my $self = shift;

    if ($self->{storage})
    {
	$self->{statement}->finish;
	$self->{storage}->close_connection($self->{connection});
	%$self = ();
    }
}

1;
