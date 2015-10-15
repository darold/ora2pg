%define uname ora2pg
%define wname ora2pg
%define _unpackaged_files_terminate_build 0

Name: %{wname}
Epoch: 0
Version: 16.r0
Release: 1%{?dist}
Summary: Oracle to PostgreSQL database schema converter

Group: Database
License: GPLv3+
URL: http://ora2pg.darold.net/
Source0: http://downloads.sourceforge.net/%{name}/%{uname}-%{version}.tar.bz2
BuildArch: noarch
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

Requires: perl(:MODULE_COMPAT_%(eval "`%{__perl} -V:version`"; echo $version))

%description
This package contains all Perl modules and scripts to convert an
Oracle or MySQL database schema, data and stored procedures to a
PostgreSQL database.

%prep
%setup -q -n %{uname}-%{version}

%build
# Make Perl and Ora2Pg distrib files
%{__perl} Makefile.PL \
    INSTALLDIRS=vendor \
    QUIET=1 \
    CONFDIR=%{_sysconfdir} \
    DOCDIR=%{_docdir}/%{wname}-%{version} \
    DESTDIR=%{buildroot} < /dev/null
%{__make}


# nope, gotta love perl

%install
%{__rm} -rf %{buildroot}
# set up path structure
%{__install} -d -m 0755 %{buildroot}/%{_bindir}
%{__install} -d -m 0755 %{buildroot}/%{_sysconfdir}/%{wname}


# Make distrib files
%{__make} install \
	DESTDIR=%{buildroot}

%{__install} -D -m 0644 doc/%{wname}.3 \
    %{buildroot}/%{_mandir}/man3/%{wname}.3

# Remove unpackaged files.
rm -f `find %{buildroot}/%{_libdir}/perl*/ -name perllocal.pod -type f`
rm -f `find %{buildroot}/%{_libdir}/perl*/ -name .packlist -type f`

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(0644,root,root,0755)
%doc change* INSTALL README
%attr(0755,root,root) %{_bindir}/%{wname}
%attr(0755,root,root) %{_bindir}/%{wname}_scanner
%attr(0644,root,root) %{_mandir}/man3/%{wname}.3.gz
%config(noreplace) %{_sysconfdir}/%{wname}/%{wname}.conf.dist
%{perl_vendorlib}/Ora2Pg/GEOM.pm
%{perl_vendorlib}/Ora2Pg/MySQL.pm
%{perl_vendorlib}/Ora2Pg/PLSQL.pm
%{perl_vendorlib}/Ora2Pg.pm

%changelog
* Tue Oct 15 2015 Gilles Darold <gilles@darold.net>
- Add MySQL.pm module and ora2pg_scanner

* Fri May 07 2010 Gilles Darold <gilles@darold.net>
- Change uname/package to be full lower case.

* Fri Feb 26 2010 Gilles Darold <gilles@darold.net>
- first packaging attempt.

