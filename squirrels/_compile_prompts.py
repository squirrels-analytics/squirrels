from typing import Tuple, Optional
import inquirer

from ._project import SquirrelsProject
from . import _constants as c, _models as m


def prompt_compile_options(
    project: SquirrelsProject,
    *,
    buildtime_only: bool,
    runtime_only: bool,
    test_set: Optional[str],
    do_all_test_sets: bool,
    selected_model: Optional[str],
) -> Tuple[bool, bool, Optional[str], bool, Optional[str]]:
    """
    Interactive prompts to derive compile options when --yes is not provided.

    Returns updated values for:
    (buildtime_only, runtime_only, test_set, do_all_test_sets, selected_model)
    """
    # 1) Scope prompt (skip if explicit flags provided)
    if not buildtime_only and not runtime_only:
        scope_answer = inquirer.prompt([
            inquirer.List(
                'scope',
                message='Select scope',
                choices=[
                    ('All models', 'all'),
                    ('Buildtime models only', 'buildtime'),
                    ('Runtime models only', 'runtime'),
                ],
                default='all',
            )
        ]) or {}
        scope_val = scope_answer['scope']
        if scope_val == 'buildtime':
            buildtime_only, runtime_only = True, False
        elif scope_val == 'runtime':
            buildtime_only, runtime_only = False, True
        else:
            buildtime_only, runtime_only = False, False

    # 2) Runtime test set prompt (only if runtime is included and -t/-T not provided)
    runtime_included, buildtime_included = not buildtime_only, not runtime_only
    if runtime_included and not test_set and not do_all_test_sets:
        test_mode_answer = inquirer.prompt([
            inquirer.List(
                'ts_mode',
                message='Runtime selections for parameters, configurables, and user attributes?',
                choices=[
                    ('Use default selections', 'default'),
                    ('Use a custom test set', 'custom'),
                    ('Use all test sets (including default selections)', 'all'),
                ],
                default='default',
            )
        ]) or {}
        ts_mode = test_mode_answer['ts_mode']
        if ts_mode == 'all':
            do_all_test_sets = True
        elif ts_mode == 'custom':
            # Build available test set list
            ts_names = list(project._manifest_cfg.selection_test_sets.keys())
            ts_answer = inquirer.prompt([
                inquirer.List(
                    'test_set',
                    message='Pick a selection test set',
                    choices=ts_names if ts_names else [c.DEFAULT_TEST_SET_NAME],
                    default=c.DEFAULT_TEST_SET_NAME,
                )
            ]) or {}
            test_set = ts_answer['test_set']

    # 3) Model selection prompt (only if -s not provided)
    if not selected_model:
        # Ask whether to compile all or a specific model
        model_mode_answer = inquirer.prompt([
            inquirer.List(
                'model_mode',
                message='Compile all models in scope or just a specific model?',
                choices=[
                    ('All models', 'all'),
                    ('Select a specific model', 'one'),
                ],
                default='all',
            )
        ]) or {}
        model_mode = model_mode_answer['model_mode']

        if model_mode == 'one':
            # Build list of runtime query models (dbviews + federates) and build models if included
            models_dict = project._get_models_dict(always_python_df=False)

            valid_model_types: list[m.ModelType] = []
            if runtime_included:
                valid_model_types.extend([m.ModelType.DBVIEW, m.ModelType.FEDERATE])
            if buildtime_included:
                valid_model_types.append(m.ModelType.BUILD)

            runtime_names = sorted([
                (f"({model.model_type.value}) {name}", name) for name, model in models_dict.items()
                if isinstance(model, m.QueryModel) and model.model_type in valid_model_types
            ])
            if runtime_names:
                model_answer = inquirer.prompt([
                    inquirer.List(
                        'selected_model',
                        message='Pick a runtime model to compile',
                        choices=runtime_names,
                    )
                ]) or {}
                selected_model = model_answer['selected_model']

    # Tips and equivalent command without prompts
    def _maybe_quote(val: str) -> str:
        return f'"{val}"' if any(ch.isspace() for ch in val) else val

    parts = ["sqrl", "compile", "-y"]
    if buildtime_only:
        parts.append("--buildtime-only")
    elif runtime_only:
        parts.append("--runtime-only")
    if do_all_test_sets:
        parts.append("-T")
    elif test_set:
        parts.extend(["-t", _maybe_quote(test_set)])
    if selected_model:
        parts.extend(["-s", _maybe_quote(selected_model)])

    # Pretty tips block
    tips_header = " Compile tips "
    border = "=" * 80
    print(border)
    print(tips_header)
    print("-" * len(border))
    print("Equivalent command (no prompts):")
    print(f"  $ {' '.join(parts)}")
    print()
    print("You can also:")
    print("  - Add -c/--clear to clear the 'target/compile/' folder before compiling")
    print("  - Add -r/--runquery to generate CSVs for runtime models")
    print(border)
    print()

    return buildtime_only, runtime_only, test_set, do_all_test_sets, selected_model
