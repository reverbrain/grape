Summary:	Grape
Name:		grape
Version:	0.7.10
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
BuildRequires:  libxml2-devel libev-devel
BuildRequires:	boost%{boost_ver}-devel, boost%{boost_ver}-iostreams, boost%{boost_ver}-system, boost%{boost_ver}-thread
BuildRequires:  curl-devel
BuildRequires:	cmake uriparser-devel

Obsoletes: srw

%description
Realtime pipeline processing engine


%package devel
Summary: Development files for %{name}
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}


%description devel
Realtime pipeline processing engine. Development files

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
%{_libdir}/grape/*
%{_libdir}/cocaine/queue-driver.cocaine-plugin

%files devel
%defattr(-,root,root,-)
%{_includedir}/grape/*
%{_libdir}/*grape*.so


%changelog

* Wed Feb 12 2014 Ivan Chelubeev <ijon@yandex-team.ru> - 0.7.10
- initial spec
