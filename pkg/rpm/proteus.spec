%{!?proteus_php_version: %global proteus_php_version 8.3}
%global proteus_php_version_nodot %(echo %{proteus_php_version} | tr -d .)
%global debug_package %{nil}

Name:           proteus
Version:        0.1.0
Release:        1%{?dist}
Summary:        Proteus PHP application server

License:        TODO-set-real-license
URL:            https://github.com/pavelkovar/proteus
Source0:        %{name}-%{version}.tar.gz
Source1:        proteus.service
Source2:        config.json

BuildRequires:  cargo
BuildRequires:  rust
BuildRequires:  gcc
BuildRequires:  make
BuildRequires:  php-devel
BuildRequires:  pkgconf-pkg-config
BuildRequires:  systemd-rpm-macros

Requires:       %{name}-php-mod%{?_isa} = %{version}-1.php%{proteus_php_version_nodot}%{?dist}
Requires(pre):  shadow-utils

%description
Proteus is PHP application server

%package -n %{name}-php-mod
Summary:        PHP embed SAPI module for Proteus
Release:        1.php%{proteus_php_version_nodot}%{?dist}
BuildRequires:  php-devel
Requires:       php-embedded

%description -n %{name}-php-mod
PHP embed SAPI module for Proteus

%prep
%autosetup -n %{name}-%{version}

%build
actual_php_version="$(php-config --version | cut -d. -f1-2)"
if [ "$actual_php_version" != "%{proteus_php_version}" ]; then
    echo "error: building proteus-php-mod for proteus_php_version=%{proteus_php_version} but php-config reports $actual_php_version" >&2
    exit 1
fi

cd server
cargo build --release --bin proteus
cd ..

make -C php-mod

%install
install -D -m 0755 server/target/release/proteus              %{buildroot}%{_bindir}/proteus
install -D -m 0755 php-mod/libproteus-php-mod.so %{buildroot}%{_libdir}/libproteus-php-mod.so
install -D -m 0644 %{SOURCE1}                                 %{buildroot}%{_unitdir}/proteus.service
install -D -m 0644 %{SOURCE2}                                 %{buildroot}%{_sysconfdir}/proteus/config.json

%pre
getent group phpapp >/dev/null || groupadd -r phpapp
getent passwd phpapp >/dev/null || useradd -r -g phpapp -d /var/lib/proteus -s /sbin/nologin -c "Proteus PHP worker user" phpapp
exit 0

%post
%systemd_post proteus.service

%preun
%systemd_preun proteus.service

%postun
%systemd_postun_with_restart proteus.service

%post -n %{name}-php-mod -p /sbin/ldconfig
%postun -n %{name}-php-mod -p /sbin/ldconfig

%files
%{_bindir}/proteus
%{_unitdir}/proteus.service
%dir %{_sysconfdir}/proteus
%config(noreplace) %{_sysconfdir}/proteus/config.json

%files -n %{name}-php-mod
%{_libdir}/libproteus-php-mod.so
