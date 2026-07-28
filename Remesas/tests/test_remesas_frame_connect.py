from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from data.legacy_persistence_repository import LegacyPersistenceRepository
from ui import remesas_frame


def _frame_double():
    connection = object()
    connection_factory = MagicMock(return_value=connection)
    frame = SimpleNamespace(
        db=SimpleNamespace(connect_fruta_with_eepp=connection_factory,
                           status=MagicMock(return_value={})),
        config=SimpleNamespace(persistence_enabled=True,
                               persistence_database_path="liquidaciones.sqlite"),
        conn=None,
        sync_results=[],
        context_panel=SimpleNamespace(
            campaña_cb={}, set_status=MagicMock()),
        master_repository=object(),
        _refresh_database_status=MagicMock(),
        _refresh_action_states=MagicMock(),
    )
    return frame, connection, connection_factory


def test_remesas_frame_imports_real_legacy_repository():
    assert remesas_frame.LegacyPersistenceRepository is LegacyPersistenceRepository


def test_connect_builds_csv_legacy_repository_with_connection_factory():
    frame, connection, connection_factory = _frame_double()
    persistence_service = MagicMock()
    persistence_service.database = object()
    legacy_repository = object()

    with (
        patch.object(remesas_frame, "MetadataRepository"),
        patch.object(remesas_frame, "ContextService") as context_service,
        patch.object(remesas_frame, "VarietyRepository"),
        patch.object(remesas_frame, "VarietyGroupService"),
        patch.object(remesas_frame, "DeliveriesRepository"),
        patch.object(remesas_frame, "DeliveriesService"),
        patch.object(remesas_frame, "RemesasRepository"),
        patch.object(remesas_frame, "RemesasService"),
        patch.object(remesas_frame, "PersistenceDatabase"),
        patch.object(remesas_frame, "LiquidationPersistenceService", return_value=persistence_service),
        patch.object(remesas_frame, "LiquidationRepository"),
        patch.object(remesas_frame, "DocumentGenerationService"),
        patch.object(remesas_frame, "LiquidationModificationService"),
        patch.object(remesas_frame, "LegacyPersistenceRepository", return_value=legacy_repository) as legacy_class,
        patch.object(remesas_frame, "LiquidationCsvExportService") as csv_service,
        patch.object(remesas_frame, "LiquidationHistoryService"),
        patch.object(remesas_frame, "LiquidationMasterRepository"),
        patch.object(remesas_frame, "HectareFeeCropRepository"),
        patch.object(remesas_frame, "HectareFeeMasterService"),
        patch.object(remesas_frame, "CalculationService"),
        patch.object(remesas_frame.messagebox, "showerror") as showerror,
    ):
        context_service.return_value.campaigns.return_value = ()
        remesas_frame.RemesasFrame._connect(frame)

    assert frame.conn is connection
    # The initial working connection is opened once; CSV initialization must not
    # invoke the factory or pass that thread-bound connection to the repository.
    connection_factory.assert_called_once_with()
    legacy_class.assert_called_once_with(connection_factory)
    assert csv_service.call_args.args[1] is legacy_repository
    assert frame.persistence_enabled is True
    showerror.assert_not_called()


def test_legacy_repository_failure_is_not_reported_as_database_sync_failure():
    frame, _, _ = _frame_double()
    persistence_service = MagicMock(database=object())

    with (
        patch.object(remesas_frame, "MetadataRepository"),
        patch.object(remesas_frame, "ContextService"),
        patch.object(remesas_frame, "VarietyRepository"),
        patch.object(remesas_frame, "VarietyGroupService"),
        patch.object(remesas_frame, "DeliveriesRepository"),
        patch.object(remesas_frame, "DeliveriesService"),
        patch.object(remesas_frame, "RemesasRepository"),
        patch.object(remesas_frame, "RemesasService"),
        patch.object(remesas_frame, "PersistenceDatabase"),
        patch.object(remesas_frame, "LiquidationPersistenceService", return_value=persistence_service),
        patch.object(remesas_frame, "LiquidationRepository"),
        patch.object(remesas_frame, "DocumentGenerationService"),
        patch.object(remesas_frame, "LiquidationModificationService"),
        patch.object(remesas_frame, "LegacyPersistenceRepository", side_effect=RuntimeError("fallo auxiliar")),
        patch.object(remesas_frame.messagebox, "showerror") as showerror,
    ):
        remesas_frame.RemesasFrame._connect(frame)

    title, message = showerror.call_args.args
    assert title == "Repositorio de datos auxiliares"
    assert "No se pudo inicializar el repositorio de datos auxiliares" in message
    assert "fallo auxiliar" in message
    assert "preparar las bases de datos" not in message
