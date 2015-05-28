%define dist    %{expand:%%(/usr/lib/rpm/redhat/dist.sh --dist)}
%define name python-pymongor
%define version 0.4
%define release 2
Summary: A utility to curate mongo databases
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{version}.tar.gz
Source1: rotate_mongodb.cron
Source2: mongor_manage.py
License: MIT
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: Lockheed Martin
Requires: python-pymongo >= 3.0


%description
Distributed database expansion to MongoDB designed to optimize scale-out, write intensive document storage


%package manager
Summary: Manager commands for the mongor
Requires: python-argparse python-pymongor


%description manager
Provides
usage: MongoR manager [-h] --host CONFIG_HOST --port CONFIG_PORT [--ssl]

                      {addnode,removenode,removeindex,addindex,listnodes,setdbtags,buildindex}
                      ...

positional arguments:
  {addnode,removenode,removeindex,addindex,listnodes,setdbtags,buildindex}
                        python manage.py <command> -h
    addnode             adds a node to mongor
    removenode          removes a node to mongor
    addindex            adds an index to mongor
    removeindex         removes index from future built buckets
    buildindex          build the index on all databases
    setdbtags           sets the db_tags field for an existing node
    listnodes           list the current mongor configuration

optional arguments:
  -h, --help            show this help message and exit
  --host CONFIG_HOST
  --port CONFIG_PORT
  --ssl


%prep
cp -fp %{SOURCE1} ./
cp -fp %{SOURCE2} ./
%setup -n %{name}-%{version} -n %{name}-%{version}


%build
python setup.py build


%install
python setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

mkdir -p $RPM_BUILD_ROOT/etc/cron.d
install -m 755 -p $RPM_BUILD_DIR/rotate_mongodb.cron \
        $RPM_BUILD_ROOT/etc/cron.d/rotate_mongodb.cron

mkdir -p $RPM_BUILD_ROOT/usr/bin/
install -m 555 -p $RPM_BUILD_DIR/mongor_manage.py \
        $RPM_BUILD_ROOT/usr/bin/mongor_manage


%clean
rm -rf $RPM_BUILD_ROOT


%files -f INSTALLED_FILES
%defattr(0644,root,root)
%config(noreplace) /etc/cron.d/rotate_mongodb.cron


%files manager
%attr(500, root, root) /usr/bin/mongor_manage
