package Ora2Pg::GEOM;
#------------------------------------------------------------------------------
# Project  : Oracle to PostgreSQL database schema converter
# Name     : Ora2Pg/GEOM.pm
# Language : Perl
# Authors  : Gilles Darold, gilles _AT_ darold _DOT_ net
# Copyright: Copyright (c) 2000-2022 : Gilles Darold - All rights reserved -
# Function : Perl module used to convert Oracle SDO_GEOMETRY into PostGis
# Usage    : See documentation
#------------------------------------------------------------------------------
#
#        This program is free software: you can redistribute it and/or modify
#        it under the terms of the GNU General Public License as published by
#        the Free Software Foundation, either version 3 of the License, or
#        any later version.
# 
#        This program is distributed in the hope that it will be useful,
#        but WITHOUT ANY WARRANTY; without even the implied warranty of
#        MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#        GNU General Public License for more details.
# 
#        You should have received a copy of the GNU General Public License
#        along with this program. If not, see < http://www.gnu.org/licenses/ >.
# 
#------------------------------------------------------------------------------
#
# Most of this work is inspired from the JTS Topology Suite developed by
# Vivid Solutions, Inc. (http://www.vividsolutions.com/jts/JTSHome.htm)
# JTS is an open source (under the LGPL license) Java library.
# See http://www.gnu.org/copyleft/lesser.html for the license.
#
#------------------------------------------------------------------------------
#
# Special thanks to Dominique Legendre and The French Geological Survey - BRGM
# http://www.brgm.eu/ and Olivier Picavet from Oslandia http://www.oslandia.com/
# who help me a lot with spatial understanding and their testing efforts.
#
#------------------------------------------------------------------------------
use vars qw($VERSION);

use strict;

$VERSION = '23.2';

# SDO_ETYPE
# Second element of triplet in SDO_ELEM_INFO
my %SDO_ETYPE = (
	# code representing Point
	'POINT' => 1,
	# code representing Line
	'LINESTRING' => 2,
	# code representing Polygon
	'POLYGON' => 3,
	# code representing compound line
	'COMPOUNDCURVE' => 4,
	# code representing exterior counterclockwise polygon ring
	'POLYGON_EXTERIOR' => 1003,
	# code representing interior clockwise polygon ring
	'POLYGON_INTERIOR' => 2003,
	# code repersenting compound polygon counterclockwise polygon ring
	'COMPOUND_POLYGON_EXTERIOR' => 1005,
	# code repersenting compound polygon clockwise polygon ring
	'COMPOUND_POLYGON_INTERIOR' => 2005,
);

# SDO_GTYPE
# Type of the geometry
my %SDO_GTYPE = (
	# Point
	'POINT' => 1,
	# Line or Curve
	'LINESTRING' => 2,
	# Polygon
	'POLYGON' => 3,
	# Geometry collection
	'GEOMETRYCOLLECTION' => 4,
	# Multpoint
	'MULTIPOINT' => 5,
	# Multiline or Multicurve
	'MULTILINESTRING' => 6,
	# Multipolygon
	'MULTIPOLYGON' => 7
);

# SDO_INTERPRETATIONS
# Third element of triplet in SDO_ELEM_INFO
# applies to points - sdo_etype 1
my %INTERPRETATION_POINT = (
	'0' => 'ORIENTED_POINT',
	'1' => 'SIMPLE_POINT'
	# n > 1: point cluster with n points
);

# applies to lines - sdo_etype 2
my %INTERPRETATION_LINE = (
	'1' => 'STRAIGHT_SEGMENTS',
	'2' => 'CURVED_SEGMENTS'
);

# applies to polygons - sdo_etypes 1003 and 2003           
my %INTERPRETATION_MULTI = (
	'1' => 'SIMPLE_POLY',
	'2' => 'ARCS_POLY',
	'3' => 'RECTANGLE',
	'4' => 'CIRCLE'
);

sub new
{
	my ($class, %options) = @_;

	# This create an OO perl object
	my $self = {};
	bless ($self, $class);

	# Initialize this object
	$self->_init(%options);

	# Return the instance
	return($self);
}

sub _init
{
	my ($self, %opt) = @_;

	$self->{dimension} = $opt{dimension} || -1;
	$self->{srid} = $opt{srid} || -1;
	$self->{geometry} = undef;

}

sub parse_sdo_geometry
{
        my ($self, $sdo_geom) = @_;

	# SDO_GEOMETRY DEFINITION
	# CREATE TYPE sdo_geometry AS OBJECT (
	#  SDO_GTYPE NUMBER, 
	#  SDO_SRID NUMBER,
	#  SDO_POINT SDO_POINT_TYPE,
	#  SDO_ELEM_INFO SDO_ELEM_INFO_ARRAY,
	#  SDO_ORDINATES SDO_ORDINATE_ARRAY
	# );
	#CREATE TYPE sdo_point_type AS OBJECT (
	#   X NUMBER,
	#   Y NUMBER,
	#   Z NUMBER);
	#CREATE TYPE sdo_elem_info_array AS VARRAY (1048576) of NUMBER;
	#CREATE TYPE sdo_ordinate_array AS VARRAY (1048576) of NUMBER;
	#
	# SDO_ELEM_INFO
	# Each triplet set of numbers is interpreted as follows:
	# SDO_STARTING_OFFSET -- Indicates the offset within the SDO_ORDINATES array where the first
	#               ordinate for this element is stored. Offset values start at 1 and not at 0.
	# SDO_ETYPE -- Indicates the type of the element.
	# SDO_interpretation -- Means one of two things, depending on whether or not SDO_ETYPE is a compound element.
	# If SDO_ETYPE is a compound element (4, 1005, or 2005), this field specifies how many subsequent
	# triplet values are part of the element.
	# If the SDO_ETYPE is not a compound element (1, 2, 1003, or 2003), the interpretation attribute determines how
	# the sequence of ordinates for this element is interpreted.

	return undef if ($#{$sdo_geom} < 0);

        # Get dimension and geometry type
        if ($sdo_geom->[0] =~ /^(\d)(\d)(\d{2})$/) {
                $self->{geometry}{sdo_gtype} = $sdo_geom->[0] || 0;
		# Extract the geometry dimension this is represented as the leftmost digit
                $self->{geometry}{dim} = $1;
		# Extract the linear referencing system
                $self->{geometry}{lrs} = $2;
		# Extract the geometry template type this is represented as the rightmost two digits
                $self->{geometry}{gtype} = $3;
		if ($self->{geometry}{dim} < 2) {
			$self->logit("ERROR: Dimension $self->{geometry}{dim} is not valid. Either specify a dimension or use Oracle Locator Version 9i or later.\n");
			return undef;
		}
        } else {
                $self->logit("ERROR: wrong SDO_GTYPE format in SDO_GEOMETRY data\n", 0, 0);
		return undef;
        }

	# Set EWKT geometry dimension
	$self->{geometry}{suffix} = '';
	if ($self->{geometry}{dim} == 3) {
		$self->{geometry}{suffix} = 'Z';
	} elsif ($self->{geometry}{dim} == 4) {
		$self->{geometry}{suffix} = 'ZM';
	}

        # Get the srid from the data otherwise it will be
	# overriden by the column srid found in meta information
	$self->{geometry}{srid} = $sdo_geom->[1] if (defined $sdo_geom->[1] && $sdo_geom->[1] ne '');
	$self->{geometry}{srid} = $self->{srid} if ($self->{geometry}{srid} eq '');

        # Look at point only coordonate
	@{$self->{geometry}{sdo_point}} = ();
        if ($sdo_geom->[2] =~ /^ARRAY\(0x/) {
		map { if (/^[-\d]+$/) { s/,/\./; s/$/\.0/; } } @{$sdo_geom->[2]};
                push(@{$self->{geometry}{sdo_point}}, @{$sdo_geom->[2]});
        }

        # Extract elements info by triplet
	@{$self->{geometry}{sdo_elem_info}} = ();
        if ($sdo_geom->[3] =~ /^ARRAY\(0x/) {
		push(@{$self->{geometry}{sdo_elem_info}}, @{$sdo_geom->[3]});
        }
        # Extract ordinates information as arrays of dimension elements
	@{$self->{geometry}{sdo_ordinates}} = ();
        if ($sdo_geom->[4] =~ /^ARRAY\(0x/) {
		map { if (/^[-\d]+$/) { s/,/\./; s/$/\.0/; } } @{$sdo_geom->[4]};
		push(@{$self->{geometry}{sdo_ordinates}}, @{$sdo_geom->[4]});
        }

	return $self->extract_geometry();

}

# Extract geometries
sub extract_geometry
{
	my ($self, %geometry) = @_;

        my @coords = ();

	# Extract coordinates following the dimension
	if ( ($self->{geometry}{gtype} == 1) && ($#{$self->{geometry}{sdo_point}} >= 0) && ($#{$self->{geometry}{sdo_elem_info}} == -1) ) {
		# Single Point Type Optimization
		@coords = $self->coordinates(@{$self->{geometry}{sdo_point}});
		@{$self->{geometry}{sdo_elem_info}} = ( 1, $SDO_ETYPE{POINT}, 1 );
	} else {
		@coords = $self->coordinates(@{$self->{geometry}{sdo_ordinates}});
	}

	# Get the geometry
	if ($self->{geometry}{gtype} == $SDO_GTYPE{POINT}) {
		return $self->createPoint(0, \@coords);
	}
	if ($self->{geometry}{gtype} == $SDO_GTYPE{LINESTRING}) {
		if ($self->{geometry}{sdo_elem_info}->[1] == $SDO_ETYPE{COMPOUNDCURVE}) {
			return $self->createCompoundLine(1, \@coords, -1);
		} else {
			return $self->createLine(0, \@coords);
		}
	}
	if ($self->{geometry}{gtype} == $SDO_GTYPE{POLYGON}) {
		return $self->createPolygon(0, \@coords);
	}
	if ($self->{geometry}{gtype} == $SDO_GTYPE{MULTIPOINT}) {
		return $self->createMultiPoint(0, \@coords);
	}
	if ($self->{geometry}{gtype} == $SDO_GTYPE{MULTILINESTRING}) {
		return $self->createMultiLine(0, \@coords, -1);
	}
	if ($self->{geometry}{gtype} == $SDO_GTYPE{MULTIPOLYGON}) {
		return $self->createMultiPolygon(0, \@coords, -1);
	}
	if ($self->{geometry}{gtype} == $SDO_GTYPE{GEOMETRYCOLLECTION}) {
		return $self->createCollection(0, \@coords,-1);
	}
}

# Build an array of references arrays of coordinates following the dimension
sub coordinates
{
	my ($self, @ordinates) = @_;

	my @coords = ();
	my @tmp = ();

	# The number of ordinates per coordinate is taken from the dimension
	for (my $i = 1; $i <= $#ordinates + 1; $i++) {
		push(@tmp, $ordinates[$i - 1]);
		if ($i % $self->{geometry}{dim} == 0) {
			push(@coords, [(@tmp)]);
			@tmp = ();
		}
	}

	return @coords;
}

# Accesses the starting index in the ordinate array for the current geometry
sub get_start_offset
{
	my ($self, $t_idx) = @_;

	if ((($t_idx * 3) + 0) >= ($#{$self->{geometry}{sdo_elem_info}} + 1)) {
		return -1;
	}

	return $self->{geometry}{sdo_elem_info}->[($t_idx * 3) + 0];
}

# Get the SDO_ETYPE part from the elemInfo triplet
sub eType
{
	my ($self, $t_idx) = @_;

	if ((($t_idx * 3) + 1) >= ($#{$self->{geometry}{sdo_elem_info}}+1)) {
		return -1;
	}

	return $self->{geometry}{sdo_elem_info}->[($t_idx * 3) + 1];
}


# Get the interpretation part  the elemInfo triplet
sub interpretation
{
	my ($self, $t_idx) = @_;

	if ((($t_idx * 3) + 2) >= ($#{$self->{geometry}{sdo_elem_info}}+1)) {
		return -1;
	}

	return $self->{geometry}{sdo_elem_info}->[($t_idx * 3) + 2];
}

# Create Geometry Collection as encoded by elemInfo.
sub createCollection
{
	my ($self, $elemIndex, $coords, $numGeom) = @_;

    	my $sOffset = $self->get_start_offset($elemIndex);

        my $length = ($#{$coords}+1) * $self->{geometry}{dim};

	if ($sOffset > $length) {
		$self->logit("ERROR: SDO_ELEM_INFO for Collection starting offset $sOffset inconsistent with ordinates length $length");
	}

        my $endTriplet = ($#{$self->{geometry}{sdo_elem_info}}+1) / 3;

	my @list_geom = ();
	my $etype;
	my $interpretation;
	my $geom;

	my $cont = 1;
	for (my $i = $elemIndex; $cont && $i < $endTriplet; $i++) {

		$etype = $self->eType($i);
		$interpretation = $self->interpretation($i);

		# Exclude type 0 (zero) element
		if ($etype == 0)
		{
			$self->logit("WARNING: SDO_ETYPE $etype not supported EWKT Geometry by Ora2Pg. Check what's going wrong with this geometry.");
			next;
		}

		if ($etype == -1) {
			$cont = 0; # We reach the end of the list - get out of here
			next;

		} elsif ($etype == $SDO_ETYPE{POINT}) {

			if ($interpretation == 1) {
				$geom = $self->createPoint($i, $coords);
			} elsif ($interpretation > 1) {
				$geom = $self->createMultiPoint($i, $coords);
			}

		} elsif ($etype == $SDO_ETYPE{LINESTRING}) {

			$geom = $self->createLine($i, $coords);

		} elsif ( ($etype == $SDO_ETYPE{POLYGON}) || ($etype == $SDO_ETYPE{POLYGON_EXTERIOR}) || ($etype == $SDO_ETYPE{POLYGON_INTERIOR}) ) {

			$geom = $self->createPolygon($i, $coords);
		} elsif ( ($etype == $SDO_ETYPE{COMPOUND_POLYGON_EXTERIOR}) || ($etype == $SDO_ETYPE{COMPOUND_POLYGON_INTERIOR}) ) {
			$geom = $self->createCompoundPolygon($i, $coords);
			# Skip elements
			$i += $interpretation;
		} else {
			$self->logit("ERROR: SDO_ETYPE $etype not representable as a EWKT Geometry by Ora2Pg.");
			next;
		}
		push(@list_geom, $geom);
	}

	return "GEOMETRYCOLLECTION$self->{geometry}{suffix} (" . join(', ', @list_geom) . ')';
}

# Create MultiPolygon
sub createMultiPolygon
{
	my ($self, $elemIndex, $coords, $numGeom) = @_;

	my $sOffset = $self->get_start_offset($elemIndex);
	my $etype = $self->eType($elemIndex);
	my $interpretation = $self->interpretation($elemIndex);

	while ($etype == 0) {
		$elemIndex++;
		$sOffset = $self->get_start_offset($elemIndex);
		$etype = $self->eType($elemIndex);
		$interpretation = $self->interpretation($elemIndex);
	}


	my $length = ($#{$coords} + 1) * $self->{geometry}{dim};

	if (($sOffset < 1) || ($sOffset > $length)) {
		$self->logit("ERROR: SDO_ELEM_INFO for MultiPolygon starting offset $sOffset inconsistent with ordinates length $length");
	}
	#  For SDO_ETYPE values 1003 and 2003, the first digit indicates exterior (1) or interior (2)
	if (($etype != $SDO_ETYPE{POLYGON}) && ($etype != $SDO_ETYPE{POLYGON_EXTERIOR})) {
		$self->logit("ERROR: SDO_ETYPE $etype inconsistent with expected POLYGON or POLYGON_EXTERIOR");
	}
	if (($interpretation != 1) && ($interpretation != 3)) {
		return undef;
	}

	my $endTriplet = ($numGeom != -1) ? $elemIndex + $numGeom : (($#{$self->{geometry}{sdo_elem_info}}+1) / 3) + 1;

	my @list = ();
	my $cont = 1;

	for (my $i = $elemIndex; $cont && $i < $endTriplet && ($etype = $self->eType($i)) != -1; $i++) {
		# Exclude type 0 (zero) element
		next if ($etype == 0);

		if (($etype == $SDO_ETYPE{POLYGON}) || ($etype == $SDO_ETYPE{POLYGON_EXTERIOR})) {
			my $poly = $self->createPolygon($i, $coords);
			$poly =~ s/POLYGON$self->{geometry}{suffix} //;
			if ($etype != $self->eType($i-1)) {
				if ( ($etype = $SDO_ETYPE{POLYGON_INTERIOR}) && ($SDO_ETYPE{POLYGON_EXTERIOR} == $self->eType($i-1)) ) {
					$poly =~ s/^\(//;
					$list[-1] =~ s/\)$//; 
				}
			}
			push(@list, $poly);
			# Skip interior rings
			while ($self->eType($i+1) == $SDO_ETYPE{POLYGON_INTERIOR}) {
				$i++;
			}
		} else { # not a Polygon - get out here
			$cont = 0;
		}
	}

	return "MULTIPOLYGON$self->{geometry}{suffix} (" . join(', ', @list) . ')';
}

# Create MultiLineString
sub createMultiLine
{
	my ($self, $elemIndex, $coords, $numGeom) = @_;

    	my $sOffset = $self->get_start_offset($elemIndex);
        my $etype = $self->eType($elemIndex);
        my $interpretation = $self->interpretation($elemIndex);

	while ($etype == 0) {
		$elemIndex++;
		$sOffset = $self->get_start_offset($elemIndex);
		$etype = $self->eType($elemIndex);
		$interpretation = $self->interpretation($elemIndex);
	}


        my $length = ($#{$coords} + 1) * $self->{geometry}{dim};

	if (($sOffset < 1) || ($sOffset > $length)) {
		$self->logit("ERROR: SDO_ELEM_INFO for MultiLine starting offset $sOffset inconsistent with ordinates length $length");
	}
	if ($etype != $SDO_ETYPE{LINESTRING}) {
		$self->logit("ERROR: SDO_ETYPE $etype inconsistent with expected LINESTRING");
	}

	my $endTriplet = ($numGeom != -1) ? ($elemIndex + $numGeom) : (($#{$self->{geometry}{sdo_elem_info}} + 1) / 3);
	my @list = ();
	my $cont = 1;
	for (my $i = $elemIndex; $cont && $i < $endTriplet && ($etype = $self->eType($i)) != -1 ; $i++) {

		# Exclude type 0 (zero) element
		next if ($etype == 0);

		if ($etype == $SDO_ETYPE{LINESTRING}) {
			push(@list, $self->createLine($i, $coords));
		} elsif ($etype == $SDO_ETYPE{COMPOUNDCURVE}) {
			push(@list, $self->createCompoundLine(1, $coords, -1));
		} else { # not a LineString - get out of here
			$cont = 0;
		}
	}

	if ($interpretation > 1 || grep(/CIRCULARSTRING/, @list)) {
		return "MULTICURVE$self->{geometry}{suffix} (" . join(', ', @list) . ')';
	}

	map { s/LINESTRING$self->{geometry}{suffix} //; } @list;
	return "MULTILINESTRING$self->{geometry}{suffix} (" . join(', ', @list) . ')';
}

# Create MultiPoint
sub createMultiPoint
{
	my ($self, $elemIndex, $coords) = @_;

	my $sOffset = $self->get_start_offset($elemIndex);
	my $etype = $self->eType($elemIndex);
	my $interpretation = $self->interpretation($elemIndex);

	while ($etype == 0) {
		$elemIndex++;
		$sOffset = $self->get_start_offset($elemIndex);
		$etype = $self->eType($elemIndex);
		$interpretation = $self->interpretation($elemIndex);
	}

        my $length = ($#{$coords} + 1) * $self->{geometry}{dim};

	if (($sOffset < 1) || ($sOffset > $length)) {
		$self->logit("ERROR: SDO_ELEM_INFO for MultiPoint starting offset $sOffset inconsistent with ordinates length $length");
	}
	if ($etype != $SDO_ETYPE{POINT}) {
		$self->logit("ERROR: SDO_ETYPE $etype inconsistent with expected POINT");
	}

	my @point = ();
        my $start = ($sOffset - 1) / $self->{geometry}{dim};
	if ($interpretation > 1) {
		for (my $i = $start + 1; $i <= $interpretation; $i++) {
			push(@point, $self->setCoordicates($coords, $i, $i));
		}

	# Oriented point are not supported by WKT
	} elsif ($interpretation != 0) {

		# There is multiple single point
		my $cont = 1;
		for (my $i = $start + 1; $cont && ($etype = $self->eType($i - 1)) != -1; $i++) {
			# Exclude type 0 (zero) element
			next if ($etype == 0);

			next if ($self->interpretation($i - 1) == 0);
			if ($etype == $SDO_ETYPE{POINT}) {
				push(@point, $self->setCoordicates($coords, $i, $i));
			} else {
				$cont = 0;
			}
		}
	}
	my $points = "MULTIPOINT$self->{geometry}{suffix} ((" . join('), (', @point) . '))';

        return $points;
}

# Create Polygon
sub createPolygon
{
	my ($self, $elemIndex, $coords) = @_;

	my $sOffset = $self->get_start_offset($elemIndex);
	my $etype = $self->eType($elemIndex);
	my $interpretation = $self->interpretation($elemIndex);

	while ($etype == 0) {
		$elemIndex++;
		$sOffset = $self->get_start_offset($elemIndex);
		$etype = $self->eType($elemIndex);
		$interpretation = $self->interpretation($elemIndex);
	}

        if ( ($sOffset < 1) || ($sOffset > ($#{$coords} + 1) * $self->{geometry}{dim}) ) {
		$self->logit("ERROR: SDO_ELEM_INFO starting offset $sOffset inconsistent with COORDINATES length " . (($#{$coords} + 1) * $self->{geometry}{dim}) );
        }

	my $poly = '';
	if ($interpretation == 1 ) {
		$poly = "POLYGON$self->{geometry}{suffix} (" . $self->createLinearRing($elemIndex, $coords).")";
	} elsif ($interpretation == 2) {
		$poly = "CURVEPOLYGON$self->{geometry}{suffix} (" . $self->createLinearRing($elemIndex, $coords).")";
	} elsif ($interpretation == 3) {
		$poly = "POLYGON$self->{geometry}{suffix} (" . $self->createRectangle($elemIndex, $coords).")";
	} else {
		$self->logit("ERROR: Unsupported polygon type with interpretation $interpretation probably mangled");
		$poly = "POLYGON$self->{geometry}{suffix} (" . $self->createLinearRing($elemIndex, $coords).")";
	}

	return $poly;
}

# Create CompoundPolygon
# AD: Unsure whether my dataset has testcases for this
sub createCompoundPolygon
{
	my ($self, $elemIndex, $coords) = @_;

	my $sOffset = $self->get_start_offset($elemIndex);
	my $etype = $self->eType($elemIndex);
	my $interpretation = $self->interpretation($elemIndex);


	if ( ($sOffset < 1) || ($sOffset > ($#{$coords} + 1) * $self->{geometry}{dim}) ) {
		$self->logit("ERROR: SDO_ELEM_INFO for Compound Polygon starting offset $sOffset inconsistent with COORDINATES length " . (($#{$coords} + 1) * $self->{geometry}{dim}) );
	}

	my @rings = ();

	my $cont = 1;
	for (my $i = $elemIndex+1; $cont && ($etype = $self->eType($i)) != -1; $i++) {

		# Exclude type 0 (zero) element
		next if ($etype == 0);

		if ($etype == $SDO_ETYPE{LINESTRING})  {
			push(@rings, $self->createLinearRing($i, $coords));
		} else {
			$self->logit("ERROR: ETYPE $etype inconsistent with Compound Polygon" );
			if ($etype == $SDO_ETYPE{POLYGON_INTERIOR}) {
				push(@rings, $self->createLinearRing($i, $coords));
			} elsif ($etype == $SDO_ETYPE{COMPOUND_POLYGON_EXTERIOR}) {
				next;
			} elsif ($etype == $SDO_ETYPE{POLYGON}) {
				push(@rings, $self->createLinearRing($i, $coords));
			} else { # not a LinearRing - get out of here
				$cont = 0;
			}
	       }
	}

	return "POLYGON$self->{geometry}{suffix} (" . join(', ', @rings) . ')';
}

sub createRectangle
{
	my ($self, $elemIndex, $coords) = @_;

	my $sOffset = $self->get_start_offset($elemIndex);
	my $etype = $self->eType($elemIndex);
	my $interpretation = $self->interpretation($elemIndex);
	my $length = ($#{$coords} + 1) * $self->{geometry}{dim};


	if ($sOffset > $length) {
		$self->logit("ERROR: SDO_ELEM_INFO for Rectangle starting offset $sOffset inconsistent with ordinates length $length");
	}

	my $ring = '';

	my $start = ($sOffset - 1) / $self->{geometry}{dim};

	my $eOffset = $self->get_start_offset($elemIndex+1); # -1 for end
	my $end = ($eOffset != -1) ? (($eOffset - 1) / $self->{geometry}{dim}) : ($#{$coords} + 1);

	if ($etype == $SDO_ETYPE{POLYGON_EXTERIOR}) {
		$ring =
		  join(' ', @{$coords->[$start]}).
		  ','.${$coords->[$start+1]}[0].' '.${$coords->[$start]}[1].
		  ','.join(' ', @{$coords->[$start+1]}).
		  ','.${$coords->[$start]}[0].' '.${$coords->[$start+1]}[1].
		  ','.join(' ', @{$coords->[$start]});
	} else { # INTERIOR
		$ring =
		  join(' ', @{$coords->[$start]}).
		  ','.${$coords->[$start]}[0].' '.${$coords->[$start+1]}[1].
		  ','.join(' ', @{$coords->[$start+1]}).
		  ','.${$coords->[$start+1]}[0].' '.${$coords->[$start]}[1].
		  ','.join(' ', @{$coords->[$start]});
	}
	return '('. $ring. ')';
}

# Create Linear Ring for polygon
sub createLinearRing
{
	my ($self, $elemIndex, $coords) = @_;

    	my $sOffset = $self->get_start_offset($elemIndex);
        my $etype = $self->eType($elemIndex);
        my $interpretation = $self->interpretation($elemIndex);
        my $length = ($#{$coords} + 1) * $self->{geometry}{dim};

	while ($etype == 0) {
		$elemIndex++;
		$sOffset = $self->get_start_offset($elemIndex);
		$etype = $self->eType($elemIndex);
		$interpretation = $self->interpretation($elemIndex);
	}


	# Exclude type 0 (zero) element
	return if ($etype == 0);

	if ($sOffset > $length) {
		$self->logit("ERROR: SDO_ELEM_INFO for LinearRing starting offset $sOffset inconsistent with ordinates length $length");
	}

	if ( ($etype == $SDO_ETYPE{COMPOUND_POLYGON_INTERIOR}) || ($etype == $SDO_ETYPE{COMPOUND_POLYGON_EXTERIOR}) ) {
		return undef;
	}

        my $ring = '';

	my $start = ($sOffset - 1) / $self->{geometry}{dim};
	if (($etype == $SDO_ETYPE{POLYGON_EXTERIOR}) && ($interpretation == 3)) {
		my $min = $coords->[$start];
		my $max = $coords->[$start+1];
		$ring  = join(' ', @$min) . ', ' . $max->[0] . ' ' . $min->[1] . ', ';
		$ring .= join(' ', @$max) . ', ' . $min->[0] . ' ' . $max->[1] . ', ';
		$ring .= join(' ', @$min);
	} elsif (($etype == $SDO_ETYPE{POLYGON_INTERIOR}) && ($interpretation == 3)) {
		my $min = $coords->[$start];
		my $max = $coords->[$start+1];
		$ring  = join(' ', @$min) . ', ' . $min->[0] . ' ' . $max->[1] . ', ';
		$ring .= join(' ', @$max) . ', ' . $max->[0] . ' ' . $min->[1] . ', ';
		$ring .= join(' ', @$min);
	} else {
		my $eOffset = $self->get_start_offset($elemIndex+1); # -1 for end
		my $end = ($eOffset != -1) ? (($eOffset - 1) / $self->{geometry}{dim}) : ($#{$coords} + 1);
		# Polygon have the last point specified exactly the same point as the first, for others
		# the coordinates for a point designating the end of one arc and the start of the next
		# arc are not repeated in SDO_GEOMETRY but must be repeated in WKT.
		if ( ($etype != $SDO_ETYPE{POLYGON}) || ($interpretation != 1) ) {
			#$end++;
		}
		if ($interpretation == 2) {
			if ( ($etype == $SDO_ETYPE{LINESTRING}) || ($etype == $SDO_ETYPE{POLYGON_EXTERIOR}) || ($etype == $SDO_ETYPE{POLYGON_INTERIOR}) ) {
				#$end++;
			}
		}
		if ( ($self->{geometry}{sdo_elem_info}->[1] == $SDO_ETYPE{COMPOUND_POLYGON_INTERIOR}) || ($self->{geometry}{sdo_elem_info}->[1] == $SDO_ETYPE{COMPOUND_POLYGON_EXTERIOR}) ) {
			$end++;
		}
		$ring = $self->setCoordicates($coords, $start+1, $end);
		if ($interpretation == 4) {
			# With circle we have to repeat the first coordinates
			$ring .= ', ' . $self->setCoordicates($coords, $start+1, $start+1);
		}
	}

	if (($etype == $SDO_ETYPE{POLYGON_EXTERIOR}) && ($interpretation == 2)) {
		$ring = "CIRCULARSTRING$self->{geometry}{suffix} (" . $ring . ')';
	} elsif (($etype == $SDO_ETYPE{COMPOUND_POLYGON_EXTERIOR}) && ($interpretation == 2)) {
		$ring = "COMPOUNDCURVE$self->{geometry}{suffix} (" . $ring . ')';
	} elsif ( $etype == $SDO_ETYPE{LINESTRING} && ($interpretation == 2)) {
		$ring = "CIRCULARSTRING$self->{geometry}{suffix} (" . $ring . ')';
	} else {
		$ring = '(' . $ring . ')';
	}

	return $ring;
}

# Create CompoundLineString
sub createCompoundLine
{
	my ($self, $elemIndex, $coords, $numGeom) = @_;

    	my $sOffset = $self->get_start_offset($elemIndex);
        my $etype = $self->eType($elemIndex);
        my $interpretation = $self->interpretation($elemIndex);

	while ($etype == 0) {
		$elemIndex++;
		$sOffset = $self->get_start_offset($elemIndex);
		$etype = $self->eType($elemIndex);
		$interpretation = $self->interpretation($elemIndex);
	}


        my $length = ($#{$coords} + 1) * $self->{geometry}{dim};

	if (($sOffset < 1) || ($sOffset > $length)) {
		$self->logit("ERROR: SDO_ELEM_INFO for CompoundLine starting offset $sOffset inconsistent with ordinates length " . ($#{$coords} + 1));
	}
	if ($etype != $SDO_ETYPE{LINESTRING}) {
		$self->logit("ERROR: SDO_ETYPE $etype inconsistent with expected LINESTRING");
	}

	my $endTriplet = ($numGeom != -1) ? ($elemIndex + $numGeom) : (($#{$self->{geometry}{sdo_elem_info}} + 1) / 3);
	my @list = ();
	my $cont = 1;
	for (my $i = $elemIndex; $cont && $i < $endTriplet && ($etype = $self->eType($i)) != -1 ; $i++) {
		# Exclude type 0 (zero) element
		next if ($etype == 0);

		if ($etype == $SDO_ETYPE{LINESTRING}) {
			push(@list, $self->createLine($i, $coords));
		} else { # not a LineString - get out of here
			$cont = 0;
		}
	}

	return "COMPOUNDCURVE$self->{geometry}{suffix} (" . join(', ', @list) . ')';
}


# Create LineString
sub createLine
{
	my ($self, $elemIndex, $coords) = @_;

    	my $sOffset = $self->get_start_offset($elemIndex);
        my $etype = $self->eType($elemIndex);
        my $interpretation = $self->interpretation($elemIndex);

	if ($etype != $SDO_ETYPE{LINESTRING}) {
		return undef;
	}

	my $start = ($sOffset - 1) / $self->{geometry}{dim};
	my $eOffset = $self->get_start_offset($elemIndex + 1); # -1 for end
	my $end = ($eOffset != -1) ? (($eOffset - 1) / $self->{geometry}{dim}) : ($#{$coords} + 1);
	if ( $self->{geometry}{sdo_elem_info}->[1] == $SDO_ETYPE{COMPOUNDCURVE}) {
		$end++;
	}

	if ($interpretation != 1) {
		my $line = "CIRCULARSTRING$self->{geometry}{suffix} ("  . $self->setCoordicates($coords, $start+1, $end) . ')';
		return $line;
	}

	my $line = "LINESTRING$self->{geometry}{suffix} (" . $self->setCoordicates($coords, $start+1, $end) . ')';

	return $line;
}

# Create Point
sub createPoint
{
	my ($self, $elemIndex, $coords) = @_;

	my $sOffset = $self->get_start_offset($elemIndex);
	my $etype = $self->eType($elemIndex);
	my $interpretation = $self->interpretation($elemIndex);
	my $length = ($#{$coords}+1) * $self->{geometry}{dim};

	if (($sOffset < 1) || ($sOffset > $length)) {
		$self->logit("ERROR: SDO_ELEM_INFO for Point starting offset $sOffset inconsistent with ordinates length $length");
	}
	if ($etype != $SDO_ETYPE{POINT}) {
		$self->logit("ERROR: SDO_ETYPE $etype inconsistent with expected POINT");
	}
	# Point cluster
	if ($interpretation > 1) {
		return $self->createMultiPoint($elemIndex, $coords);
	# Oriented point should be processed by MULTIPOINT
	} elsif ($interpretation == 0) {
		$self->logit("ERROR: SDO_ETYPE.POINT with interpretation = 0 is not supported");
		return undef;
	}

	my $start = ($sOffset - 1) / $self->{geometry}{dim};
	my $eOffset = $self->get_start_offset($elemIndex + 1); # -1 for end
	my $end = ($eOffset != -1) ? (($eOffset - 1) / $self->{geometry}{dim}) : ($#{$coords} + 1);
	my $point = "POINT$self->{geometry}{suffix} (" . $self->setCoordicates($coords, $start+1, $end) . ')';

	return $point;
}

sub setCoordicates
{
	my ($self, $coords, $start, $end) = @_;

	my $str = '';

	$start ||= 1;
	$end = $#{$coords} + 1 if ($end <= 0);

	for (my $i = $start - 1; $i < $end && ($i <= $#{$coords}); $i++) {
		my $coordinates = join(' ', @{$coords->[$i]});
		if ($coordinates =~ /\d/) {
			$str .= "$coordinates, ";
		}
	}
	$str =~ s/, $//;

	return $str;
}

sub logit
{
	my ($self, $message, $level, $critical) = @_;

	if (defined $self->{fhlog}) {
		$self->{fhlog}->print("$message\n");
	} else {
		print "$message\n";
	}
}

1;

