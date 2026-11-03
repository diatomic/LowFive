import os
from pathlib import Path
import stat
import subprocess
import sys
import sysconfig
import tempfile

build, prefix, case, *arguments = sys.argv[1:]
libdir = next((arg.partition("=")[2] for arg in arguments
               if arg.startswith("--libdir=")), "lib")
bindir = next((arg.partition("=")[2] for arg in arguments
               if arg.startswith("--bindir=")), "bin")
includedir = next((arg.partition("=")[2] for arg in arguments
                   if arg.startswith("--includedir=")), "include")
installed_files = [arg.partition("=")[2] for arg in arguments
                   if arg.startswith("--file=")]
libraries = [arg for arg in arguments
             if not arg.startswith(("--libdir=", "--bindir=", "--includedir=", "--file="))]
configured_prefix = prefix
with tempfile.TemporaryDirectory(prefix="lowfive-install-") as temporary:
    stage = Path(temporary)
    environment = dict(os.environ, DESTDIR=temporary)
    command = ["cmake", "--install", build]
    if case == "relocatable":
        prefix = "/lowfive-relocated"
        if prefix == configured_prefix:
            prefix += "-override"
        command.extend(["--prefix", prefix])
    subprocess.run(command, env=environment, check=True,
                   stdout=subprocess.DEVNULL)
    root = stage / prefix.lstrip("/")
    if case == "permissions":
        for name in libraries:
            library = root / libdir / name
            assert library.is_file(), library
            assert not library.stat().st_mode & stat.S_IWOTH, library
    elif case == "python":
        platlib = sysconfig.get_path("platlib", vars={"base": prefix, "platbase": prefix})
        installed = stage / platlib.lstrip("/")
        environment["PYTHONPATH"] = os.pathsep.join(
            filter(None, (str(installed), environment.get("PYTHONPATH"))))
        if sys.platform == "win32":
            loader_variable = "PATH"
            library_directories = [root / bindir, root / libdir]
        elif sys.platform == "darwin":
            loader_variable = "DYLD_LIBRARY_PATH"
            library_directories = [root / libdir]
        elif sys.platform.startswith("aix"):
            loader_variable = "LIBPATH"
            library_directories = [root / libdir]
        else:
            loader_variable = "LD_LIBRARY_PATH"
            library_directories = [root / libdir]
        loader_path = [str(directory) for directory in library_directories]
        if environment.get(loader_variable):
            loader_path.append(environment[loader_variable])
        environment[loader_variable] = os.pathsep.join(loader_path)
        subprocess.run([sys.executable, "-c",
                        "import lowfive; from lowfive import _lowfive; "
                        "assert _lowfive.__file__.startswith(" + repr(str(installed)) + ")"],
                       env=environment, cwd=temporary, check=True)
    elif case == "relocatable":
        for name in libraries:
            library = root / libdir / name
            assert library.is_file(), library
        header = root / includedir / "lowfive" / "vol-metadata.hpp"
        assert header.is_file(), header
        for name in installed_files:
            installed_file = root / name
            assert installed_file.is_file(), installed_file
        configured_root = stage / configured_prefix.lstrip("/")
        configured_headers = configured_root / includedir / "lowfive"
        if installed_files:
            configured_examples = configured_root / "examples"
            assert not configured_examples.exists(), configured_examples
        assert not configured_headers.exists(), configured_headers
    elif case == "public-header":
        compiler = next(arg.partition("=")[2] for arg in libraries
                        if arg.startswith("--compiler="))
        toolchain = next(arg.partition("=")[2] for arg in libraries
                         if arg.startswith("--toolchain="))
        include_dirs = [arg.partition("=")[2] for arg in libraries
                        if arg.startswith("--include=")]
        consumer = stage / "consumer"
        forbidden = consumer / "forbidden" / "fmt"
        forbidden.mkdir(parents=True)
        (forbidden / "core.h").write_text(
            '#error "installed LowFive headers must not require fmt"\n')
        (consumer / "main.cpp").write_text(
            "#include <lowfive/vol-metadata.hpp>\n"
            "int main() { LowFive::LocationPattern pattern; return pattern.filename.size(); }\n")
        include_directories = "\n".join(
            f'    "{Path(directory).as_posix()}"' for directory in include_dirs)
        (consumer / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.15)\n"
            "project(lowfive_public_header LANGUAGES CXX)\n"
            "add_executable(consumer main.cpp)\n"
            "target_compile_features(consumer PRIVATE cxx_std_17)\n"
            "target_include_directories(consumer PRIVATE\n"
            '    "forbidden"\n'
            f'    "{(root / includedir).as_posix()}"\n'
            f"{include_directories})\n")
        consumer_build = consumer / "build"
        configure = ["cmake", "-S", str(consumer), "-B", str(consumer_build),
                     f"-DCMAKE_CXX_COMPILER={compiler}"]
        if toolchain:
            configure.append(f"-DCMAKE_TOOLCHAIN_FILE={toolchain}")
        subprocess.run(configure, env=environment, check=True,
                       stdout=subprocess.DEVNULL)
        subprocess.run(["cmake", "--build", str(consumer_build)],
                       env=environment, check=True, stdout=subprocess.DEVNULL)
