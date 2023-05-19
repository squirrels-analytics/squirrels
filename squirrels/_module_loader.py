from typing import List, Tuple
from squirrels import _constants as c, _manifest as mf
import git, shutil, os, stat


def parse_module_repo_strings(repo_strings: List[str]) -> List[Tuple[str]]:
    output = []
    for repo in repo_strings:
        try:
            url, tag = repo.split('@')
        except ValueError:
            raise RuntimeError(f'cannot split git repo "{repo}" into url and tag/branch... format must be like "url@tag"')
        
        try:
            name, url = url.split('=')
        except ValueError:
            name = url.split('/')[-1].replace('.git', '')

        output.append((name, url, tag))
    
    return output


def load_modules(manifest: mf.Manifest) -> None:
    repo_strings: List[str] = manifest.get_modules()

    # Recreate the modules directory if it exists
    if os.path.exists(c.MODULES_FOLDER):
        def del_rw(action, name, exc):
            os.chmod(name, stat.S_IWRITE)
            os.remove(name)
        shutil.rmtree(c.MODULES_FOLDER, onerror=del_rw)
    os.mkdir(c.MODULES_FOLDER)
    
    module_repos = parse_module_repo_strings(repo_strings)
    for name, url, tag in module_repos:
        git.Repo.clone_from(url, f"modules/{name}", branch=tag, depth=1)
