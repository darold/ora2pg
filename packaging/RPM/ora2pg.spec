%define uname ora2pg
%define wname ora2pg
%define _unpackaged_files_terminate_build 0

Name: %{wname}
Epoch: 0
Version: 13.0
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
This package contains a Perl module and a companion script to convert an
Oracle database schema to PostgreSQL and to migrate the data from an
Oracle database to a PostgreSQL database.

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
%doc Change* INSTALL README
%attr(0755,root,root) %{_bindir}/%{wname}
%attr(0644,root,root) %{_mandir}/man3/%{wname}.3.gz
%config(noreplace) %{_sysconfdir}/%{wname}/%{wname}.conf
%{perl_vendorlib}/Ora2Pg/PLSQL.pm
%{perl_vendorlib}/Ora2Pg.pm

%changelog
* Fri May 07 2010 Gilles Darold <gilles@darold.net>
- Change uname/package to be full lower case.

* Fri Feb 26 2010 Gilles Darold <gilles@darold.net>
- first packaging attempt.

