import git, shutil, os, time

from . import _constants as c, _utils as u
from ._manifest import ManifestIO
from ._timer import timer


class PackageLoaderIO:

    @classmethod
    def LoadPackages(cls, *, reload: bool = False) -> None:
        start = time.time()
        # If reload, delete the modules directory (if it exists). It will be recreated later
        if reload and os.path.exists(c.PACKAGES_FOLDER):
            shutil.rmtree(c.PACKAGES_FOLDER)
        
        package_repos = ManifestIO.obj.packages
        for repo in package_repos:
            target_dir = f"{c.PACKAGES_FOLDER}/{repo.directory}"
            if not os.path.exists(target_dir):
                try:
                    git.Repo.clone_from(repo.git_url, target_dir, branch=repo.revision, depth=1)
                except git.GitCommandError as e:
                    raise u.ConfigurationError(f"Git clone of package failed for this repository: {repo.git_url}") from e
        
        timer.add_activity_time("loading packages", start)
