import pathlib
import re
def dir_scanner(dir_path, skip_file_extensions=[], allowed_filename_pattern=None, get_dirs=False):
    dir_path = pathlib.Path(dir_path)
    if get_dirs:
        yield dir_path
    for sub in dir_path.iterdir():
        if sub.is_dir():
            yield from dir_scanner(sub, skip_file_extensions)
        else:
            if sub.suffix not in skip_file_extensions:
                if allowed_filename_pattern is not None:
                    try:
                        re.compile(allowed_filename_pattern)
                        if re.search(allowed_filename_pattern, sub._str):
                            yield sub._str
                    except re.error:
                        raise re.error
                else:
                    yield sub._str

