%define dist    %{expand:%%(/usr/lib/rpm/redhat/dist.sh --dist)}
%define __python /usr/bin/python
%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}
%define name python-pymongor
%define version 0.3
%define unmangled_version 0.3
%define unmangled_version 0.3
%define release 9 
Summary: A utility to curate mongo databases
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
Source1: rotate_mongodb.cron
License: UNKNOWN
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Daniel Bauman <Daniel.Bauman@lmco.com>
Requires: python python-pymongo mongodb mongodb-server python-dateutil 
%description
UNKNOWN

%prep
cp -fp %{SOURCE1} ./
%setup -n %{name}-%{unmangled_version} -n %{name}-%{unmangled_version}

%build
python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

mkdir -p $RPM_BUILD_ROOT/etc/cron.d
install -m 755 -p $RPM_BUILD_DIR/rotate_mongodb.cron \
        $RPM_BUILD_ROOT/etc/cron.d/rotate_mongodb.cron



%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(0644,root,root)
%config(noreplace) /etc/cron.d/rotate_mongodb.cron
