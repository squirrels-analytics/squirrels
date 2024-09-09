from typing import Callable, Coroutine, Any
import inspect, os

from .arguments.run_time_args import DashboardArgs
from ._timer import timer, time
from ._py_module import PyModule
from . import _constants as c, _utils as u, dashboards as d


class DashboardsIO:
    dashboards_by_name: dict[str, Callable[[DashboardArgs], Coroutine[Any, Any, d._Dashboard]]] = {}

    @classmethod
    def load_files(cls) -> None:
        start = time.time()
        
        for dp, _, filenames in os.walk(c.DASHBOARDS_FOLDER):
            for file in filenames:
                filepath = os.path.join(dp, file)
                file_stem, extension = os.path.splitext(file)
                if extension == '.py':
                    module = PyModule(filepath)
                    dashboard_func = module.get_func_or_class(c.MAIN_FUNC)
                    cls.dashboards_by_name[file_stem] = dashboard_func
        
        timer.add_activity_time("loading files for dashboards", start)

    @classmethod
    def get_dashboard_format(cls, dashboard_name: str) -> str:
        dashboard_func = cls.dashboards_by_name[dashboard_name]
        return_type = inspect.signature(dashboard_func).return_annotation
        assert issubclass(return_type, d._Dashboard), f"Function must return Dashboard type"
        if return_type == d.PngDashboard:
            return c.PNG
        elif return_type == d.HtmlDashboard:
            return c.HTML
        else:
            raise NotImplementedError(f"Dashboard format {return_type} not supported")
    
    @classmethod
    async def get_dashboard(cls, dashboard_name: str, args: DashboardArgs) -> d._Dashboard:
        dashboard_func = cls.dashboards_by_name[dashboard_name]
        try:
            dashboard = await dashboard_func(args)
            assert isinstance(dashboard, d._Dashboard), f"Function must return Dashboard type"
        except (u.InvalidInputError, u.ConfigurationError, u.FileExecutionError) as e:
            raise e
        except Exception as e:
            raise u.FileExecutionError(f'Failed to run "{c.MAIN_FUNC}" function for dashboard "{dashboard_name}"', e) from e

        return dashboard
