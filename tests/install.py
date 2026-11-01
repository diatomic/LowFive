import os
from pathlib import Path
import stat
import subprocess
import sys
import sysconfig
import tempfile

build, prefix, case, *libraries = sys.argv[1:]
with tempfile.TemporaryDirectory(prefix="lowfive-install-") as temporary:
    stage = Path(temporary)
    environment = dict(os.environ, DESTDIR=temporary)
    subprocess.run(["cmake", "--install", build], env=environment, check=True,
                   stdout=subprocess.DEVNULL)
    root = stage / prefix.lstrip("/")
    if case == "permissions":
        for name in libraries:
            library = root / "lib" / name
            assert library.is_file(), library
            assert not library.stat().st_mode & stat.S_IWOTH, library
    elif case == "python":
        platlib = sysconfig.get_path("platlib", vars={"base": prefix, "platbase": prefix})
        installed = stage / platlib.lstrip("/")
        environment["PYTHONPATH"] = str(installed)
        if sys.platform == "win32":
            loader_variable = "PATH"
            library_directories = [root / "bin", root / "lib"]
        elif sys.platform == "darwin":
            loader_variable = "DYLD_LIBRARY_PATH"
            library_directories = [root / "lib"]
        elif sys.platform.startswith("aix"):
            loader_variable = "LIBPATH"
            library_directories = [root / "lib"]
        else:
            loader_variable = "LD_LIBRARY_PATH"
            library_directories = [root / "lib"]
        loader_path = [str(directory) for directory in library_directories]
        if environment.get(loader_variable):
            loader_path.append(environment[loader_variable])
        environment[loader_variable] = os.pathsep.join(loader_path)
        subprocess.run([sys.executable, "-c",
                        "import lowfive; from lowfive import _lowfive; "
                        "assert _lowfive.__file__.startswith(" + repr(str(installed)) + ")"],
                       env=environment, cwd=temporary, check=True)
