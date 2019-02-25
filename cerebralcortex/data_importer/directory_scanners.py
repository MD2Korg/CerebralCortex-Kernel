import pathlib

def dir_scanner(dir_path, skip_file_extensions=[".json"], get_dirs=False):
    dir_path = pathlib.Path(dir_path)
    if get_dirs:
        yield dir_path
    for sub in dir_path.iterdir():
        if sub.is_dir():
            yield from dir_scanner(sub)
        else:
            if sub.suffix not in skip_file_extensions:
                yield sub._str

