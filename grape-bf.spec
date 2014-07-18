Summary:	Grape
Name:		grape
Version:	0.7.12
Release:	1%{?dist}

License:	GPLv2+
Group:		System Environment/Libraries
URL:		https://github.com/reverbrain/grape
Source0:	%{name}-%{version}.tar.bz2
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%if %{defined rhel} && 0%{?rhel} < 6
BuildRequires:	gcc44 gcc44-c++
%define boost_ver 141
%else
%define boost_ver %{nil}
%endif
BuildRequires: cmake
BuildRequires: elliptics-devel >= 2.25, elliptics-client-devel >= 2.25
BuildRequires: cocaine-framework-native-devel >= 0.11, cocaine-framework-native-devel < 0.12
BuildRequires: libcocaine-core2-devel >= 0.11, libcocaine-core2-devel < 0.12
BuildRequires: boost%{boost_ver}-devel
BuildRequires: gtest-devel
BuildRequires: msgpack-devel

#FIXME: This is proxy dependency for cocaine-framework-native,
# we only need to be able to link apps with binary libev4
BuildRequires: libev-devel

Obsoletes: srw

Requires: python-prettytable

%description
Realtime pipeline processing engine


%package devel
Summary: Development files for %{name}
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}

%description devel
Realtime pipeline processing engine (development files)


%package components
Summary: Grape queue and other components (cocaine apps)
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}

%description components
Grape queue and other component apps

%package -n cocaine-plugin-queue-driver
Summary: Grape queue driver (cocaine plugin)
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}

%description -n cocaine-plugin-queue-driver
Grape queue driver runs as a cocaine plugin and can be turned on for applications,
which want to pop events from persistent queue (application named 'queue')


%prep
%setup -q

%build
mkdir -p %{_target_platform}
pushd %{_target_platform}

%if %{defined rhel} && 0%{?rhel} < 6
export CC=gcc44
export CXX=g++44
CXXFLAGS="-pthread -I/usr/include/boost141" LDFLAGS="-L/usr/lib64/boost141" %{cmake} -DBoost_LIB_DIR=/usr/lib64/boost141 -DBoost_INCLUDE_DIR=/usr/include/boost141 -DBoost_LIBRARYDIR=/usr/lib64/boost141 -DBOOST_LIBRARYDIR=/usr/lib64/boost141 -DCMAKE_CXX_COMPILER=g++44 -DCMAKE_C_COMPILER=gcc44 ..
%else
%{cmake} ..
%endif

popd

make %{?_smp_mflags} -C %{_target_platform}

%install
rm -rf %{buildroot}
make install DESTDIR=%{buildroot} -C %{_target_platform}

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%clean
rm -rf %{buildroot}


%files
%defattr(-,root,root,-)
%{_libdir}/*grape*.so*
%{_libdir}/grape/launchpad/*

%files devel
%defattr(-,root,root,-)
%{_includedir}/grape/*
%{_libdir}/*grape*.so

%files components
%defattr(-,root,root,-)
%{_libdir}/queue
%{_libdir}/testerhead-cpp

%files -n cocaine-plugin-queue-driver
%defattr(-,root,root,-)
%{_libdir}/cocaine/queue-driver.cocaine-plugin


# Primary place for the changelog is debian/changelog,
# there is no tool to convert debian/changelog to rpm one and
# it's silly to do that by hand -- so, no changelog at all
#%changelog
