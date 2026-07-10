def test_imports() -> None:
    import proberca
    import proberca.data.schema
    import proberca.data.io
    import proberca.graph.schema
    import proberca.cli.check_project

    assert proberca.__version__


def test_step7_modules_import():
    import proberca.explain.path  # noqa: F401
    import proberca.cli.explain_paths  # noqa: F401


def test_step8_modules_import():
    import proberca.eval.p0_result  # noqa: F401
    import proberca.eval.metrics  # noqa: F401
    import proberca.eval.p0_experiment  # noqa: F401
    import proberca.cli.run_p0_experiment  # noqa: F401


def test_step8a_modules_import():
    import proberca.eval.p0_audit  # noqa: F401
    import proberca.cli.run_p0_audit  # noqa: F401


def test_step8b_modules_import():
    import proberca.eval.g1_gate  # noqa: F401
    import proberca.cli.run_g1_gate  # noqa: F401


def test_step8c_modules_import():
    import proberca.eval.p0_failure_analysis  # noqa: F401
    import proberca.cli.analyze_p0_failures  # noqa: F401
