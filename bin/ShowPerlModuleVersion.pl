#!/usr/bin/perl

use warnings;
use strict;
my @modules = @ARGV;
for my $module (@modules) {
    eval "require $module";
    if ($@) {
        my $error = $@;
        if ($error =~ /Can't locate/) {
            warn "$module is not installed.\n";
        }
        else {
            warn "$module had a problem: $@.\n";
        }
        next;
    }
    my $version = $module->VERSION ();
    print "'$module' => '$version',\n";
}
