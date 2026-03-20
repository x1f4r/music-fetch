from __future__ import annotations

from typing import Iterable

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import Button, DataTable, Footer, Header, Input, Label, Static

from .config import default_ui_language, load_user_config, save_user_config
from .context import AppContext
from .models import DetectedSegment, JobStatus, SegmentKind


class MusicFetchTUI(App[None]):
    CSS = """
    Screen {
      background: $surface;
      color: $text;
      layout: vertical;
    }

    Header {
      dock: top;
    }

    Footer {
      dock: bottom;
    }

    #shell {
      height: 1fr;
      width: 100%;
    }

    #sidebar {
      width: 28;
      min-width: 24;
      background: $panel;
      border-right: heavy $surface-lighten-1;
      padding: 1 1 1 1;
    }

    .brand {
      height: 4;
      padding: 1 1 0 1;
    }

    .brand-title {
      text-style: bold;
      color: $text;
    }

    .brand-subtitle {
      color: $text-muted;
    }

    .section-label {
      margin: 1 1 0 1;
      color: $text-muted;
      text-style: bold;
    }

    .nav-button {
      width: 100%;
      margin: 0 0 1 0;
    }

    #workspace {
      width: 1fr;
      padding: 1;
    }

    .sidebar-panel {
      display: none;
      height: 1fr;
      margin-top: 1;
    }

    .sidebar-panel.-active {
      display: block;
    }

    .pane {
      width: 100%;
      height: 1fr;
      display: none;
    }

    .pane.-active {
      display: block;
    }

    .card {
      background: $boost;
      border: round $surface-lighten-1;
      padding: 1;
      margin: 0 0 1 0;
    }

    .title {
      text-style: bold;
      margin: 0 0 1 0;
    }

    #analyze_input_row {
      height: 3;
      margin: 0 0 1 0;
    }

    #analyze_input {
      width: 1fr;
      margin-right: 1;
    }

    #analyze_actions {
      height: auto;
      margin: 0 0 1 0;
    }

    #analyze_results_shell {
      height: 1fr;
    }

    #analyze_left {
      width: 38%;
      min-width: 34;
      margin-right: 1;
    }

    #analyze_right,
    #library_right,
    #storage_right {
      width: 1fr;
    }

    DataTable {
      height: 1fr;
      border: round $surface-lighten-1;
    }

    #timeline {
      height: auto;
      color: $text-muted;
      margin: 0 0 1 0;
    }

    #status_line {
      height: auto;
      color: $text-muted;
    }

    .muted {
      color: $text-muted;
    }
    """

    BINDINGS = [
        ("ctrl+n", "new_analysis", "New analysis"),
        ("ctrl+r", "refresh", "Refresh"),
        ("1", "show_analyze", "Analyze"),
        ("2", "show_library", "Library"),
        ("3", "show_storage", "Storage"),
        ("4", "show_settings", "Settings"),
        ("q", "quit", "Quit"),
    ]

    current_section = reactive("analyze")
    selected_job_id = reactive("")
    selected_segment_id = reactive("")
    selected_storage_job_id = reactive("")
    show_only_songs = reactive(True)

    def __init__(self, context: AppContext) -> None:
        super().__init__()
        self.context = context
        self.language = (load_user_config(context.settings).get("language") or default_ui_language()).lower()

    def tr(self, en: str, de: str, es: str, fr: str) -> str:
        if self.language == "de":
            return de
        if self.language == "es":
            return es
        if self.language == "fr":
            return fr
        return en

    def set_language(self, code: str) -> None:
        self.language = code
        data = load_user_config(self.context.settings)
        data["language"] = code
        save_user_config(self.context.settings, data)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="shell"):
            with Vertical(id="sidebar"):
                yield Static(f"Music Fetch\n[dim]{self.tr('Workspace', 'Arbeitsbereich', 'Espacio', 'Espace')}[/dim]", classes="brand")
                yield Button(self.tr("New", "Neu", "Nuevo", "Nouveau"), id="new_analysis", variant="primary", classes="nav-button")
                yield Static(self.tr("Sections", "Bereiche", "Secciones", "Sections"), classes="section-label")
                yield Button(self.tr("Analyze", "Analysieren", "Analizar", "Analyser"), id="nav_analyze", classes="nav-button")
                yield Button(self.tr("Library", "Bibliothek", "Biblioteca", "Bibliothèque"), id="nav_library", classes="nav-button")
                yield Button(self.tr("Storage", "Speicher", "Almacenamiento", "Stockage"), id="nav_storage", classes="nav-button")
                yield Button(self.tr("Settings", "Einstellungen", "Ajustes", "Réglages"), id="nav_settings", classes="nav-button")
                with Vertical(id="sidebar_library_panel", classes="sidebar-panel"):
                    yield Static(self.tr("Library", "Bibliothek", "Biblioteca", "Bibliothèque"), classes="section-label")
                    yield Button(self.tr("Refresh", "Aktualisieren", "Actualizar", "Actualiser"), id="library_refresh", classes="nav-button")
                    yield DataTable(id="library_table")
                with Vertical(id="sidebar_storage_panel", classes="sidebar-panel"):
                    yield Static(self.tr("Storage", "Speicher", "Almacenamiento", "Stockage"), classes="section-label")
                    yield Button(self.tr("Refresh", "Aktualisieren", "Actualizar", "Actualiser"), id="storage_refresh", classes="nav-button")
                    yield DataTable(id="storage_scope_table")
            with Vertical(id="workspace"):
                with Vertical(id="pane_analyze", classes="pane -active"):
                    yield Static(self.tr("Analyze", "Analysieren", "Analizar", "Analyser"), classes="title")
                    with Horizontal(id="analyze_input_row"):
                        yield Input(placeholder=self.tr("Paste a link or local path", "Link oder Pfad einfügen", "Pega un enlace o ruta local", "Collez un lien ou chemin local"), id="analyze_input")
                        yield Button(self.tr("Analyze", "Analysieren", "Analizar", "Analyser"), id="analyze_button", variant="primary")
                    with Horizontal(id="analyze_actions"):
                        yield Button(self.tr("File", "Datei", "Archivo", "Fichier"), id="choose_file")
                        yield Button(self.tr("Mic", "Mikro", "Mic", "Micro"), id="record_mic")
                        yield Button(self.tr("System", "System", "Sistema", "Système"), id="record_system")
                        yield Button(self.tr("Songs", "Songs", "Canciones", "Titres"), id="toggle_songs")
                    yield Static(self.tr("Ready", "Bereit", "Listo", "Prêt"), id="status_line", classes="card")
                    with Horizontal(id="analyze_results_shell"):
                        with Vertical(id="analyze_left"):
                            yield Static(self.tr("Timeline", "Timeline", "Timeline", "Timeline"), classes="title")
                            yield Static(self.tr("No analysis yet", "Noch keine Analyse", "Sin análisis", "Pas d’analyse"), id="timeline", classes="card")
                            yield DataTable(id="segment_table")
                        with Vertical(id="analyze_right"):
                            yield Static(self.tr("Inspector", "Details", "Inspector", "Inspecteur"), classes="title")
                            yield Static(self.tr("Run analysis and pick a segment.", "Analyse starten und Segment wählen.", "Ejecuta un análisis y elige un segmento.", "Lancez une analyse et choisissez un segment."), id="segment_inspector", classes="card")
                with Vertical(id="pane_library", classes="pane"):
                    yield Static(self.tr("Library", "Bibliothek", "Biblioteca", "Bibliothèque"), classes="title")
                    with Vertical(id="library_right"):
                        with Horizontal(classes="card"):
                            yield Button(self.tr("Open", "Öffnen", "Abrir", "Ouvrir"), id="library_open")
                            yield Button(self.tr("Pin", "Pinnen", "Fijar", "Épingler"), id="library_pin")
                        yield Static(self.tr("Pick an analysis from the library.", "Analyse aus der Bibliothek wählen.", "Elige un análisis de la biblioteca.", "Choisissez une analyse."), id="library_detail", classes="card")
                with Vertical(id="pane_storage", classes="pane"):
                    yield Static(self.tr("Storage", "Speicher", "Almacenamiento", "Stockage"), classes="title")
                    with Vertical(id="storage_right"):
                        with Horizontal(classes="card"):
                            yield Button(self.tr("Clean Job", "Job löschen", "Limpiar job", "Nettoyer job"), id="storage_cleanup_selected", variant="warning")
                            yield Button(self.tr("Clean All", "Alles löschen", "Limpiar todo", "Tout nettoyer"), id="storage_cleanup_all", variant="error")
                            yield Button(self.tr("Pin", "Pinnen", "Fijar", "Épingler"), id="storage_pin_toggle")
                        yield Static(self.tr("Storage details appear here.", "Speicherdetails erscheinen hier.", "Los detalles aparecen aquí.", "Les détails s’affichent ici."), id="storage_detail", classes="card")
                with Vertical(id="pane_settings", classes="pane"):
                    yield Static(self.tr("Settings", "Einstellungen", "Ajustes", "Réglages"), classes="title")
                    with Horizontal(classes="card"):
                        yield Button("English", id="lang_en")
                        yield Button("Deutsch", id="lang_de")
                        yield Button("Español", id="lang_es")
                        yield Button("Français", id="lang_fr")
                    yield Static("", id="settings_detail", classes="card")
        yield Footer()

    def on_mount(self) -> None:
        self._init_tables()
        self.refresh_all()
        self.set_interval(2.0, self.refresh_all)

    def watch_current_section(self, _value: str) -> None:
        self._sync_section_visibility()

    def _init_tables(self) -> None:
        segment_table = self.query_one("#segment_table", DataTable)
        segment_table.cursor_type = "row"
        segment_table.add_columns(self.tr("Range", "Zeit", "Rango", "Plage"), self.tr("Type", "Typ", "Tipo", "Type"), self.tr("Title", "Titel", "Título", "Titre"))

        library_table = self.query_one("#library_table", DataTable)
        library_table.cursor_type = "row"
        library_table.add_columns(self.tr("Title", "Titel", "Título", "Titre"), self.tr("Status", "Status", "Estado", "État"), self.tr("Segments", "Segmente", "Segmentos", "Segments"), self.tr("Size", "Größe", "Tamaño", "Taille"), self.tr("Pin", "Pin", "Pin", "Pin"))

        storage_scope = self.query_one("#storage_scope_table", DataTable)
        storage_scope.cursor_type = "row"
        storage_scope.add_columns(self.tr("Scope", "Bereich", "Ámbito", "Périmètre"), self.tr("Size", "Größe", "Tamaño", "Taille"), self.tr("Segments", "Segmente", "Segmentos", "Segments"), self.tr("Pin", "Pin", "Pin", "Pin"))

    def refresh_all(self) -> None:
        self._refresh_copy()
        self._refresh_library()
        self._refresh_storage_scopes()
        self._refresh_analyze()
        self._refresh_storage_detail()
        self._refresh_settings()
        self._sync_section_visibility()

    def _refresh_copy(self) -> None:
        self.query_one("#new_analysis", Button).label = self.tr("New", "Neu", "Nuevo", "Nouveau")
        self.query_one("#nav_analyze", Button).label = self.tr("Analyze", "Analysieren", "Analizar", "Analyser")
        self.query_one("#nav_library", Button).label = self.tr("Library", "Bibliothek", "Biblioteca", "Bibliothèque")
        self.query_one("#nav_storage", Button).label = self.tr("Storage", "Speicher", "Almacenamiento", "Stockage")
        self.query_one("#nav_settings", Button).label = self.tr("Settings", "Einstellungen", "Ajustes", "Réglages")
        self.query_one("#analyze_button", Button).label = self.tr("Analyze", "Analysieren", "Analizar", "Analyser")
        self.query_one("#choose_file", Button).label = self.tr("File", "Datei", "Archivo", "Fichier")
        self.query_one("#record_mic", Button).label = self.tr("Mic", "Mikro", "Mic", "Micro")
        self.query_one("#record_system", Button).label = self.tr("System", "System", "Sistema", "Système")
        self.query_one("#toggle_songs", Button).label = self.tr("Songs", "Songs", "Canciones", "Titres")
        self.query_one("#library_open", Button).label = self.tr("Open", "Öffnen", "Abrir", "Ouvrir")
        self.query_one("#library_pin", Button).label = self.tr("Pin", "Pinnen", "Fijar", "Épingler")
        self.query_one("#library_refresh", Button).label = self.tr("Refresh", "Aktualisieren", "Actualizar", "Actualiser")
        self.query_one("#storage_cleanup_selected", Button).label = self.tr("Clean Job", "Job löschen", "Limpiar job", "Nettoyer job")
        self.query_one("#storage_cleanup_all", Button).label = self.tr("Clean All", "Alles löschen", "Limpiar todo", "Tout nettoyer")
        self.query_one("#storage_pin_toggle", Button).label = self.tr("Pin", "Pinnen", "Fijar", "Épingler")
        self.query_one("#storage_refresh", Button).label = self.tr("Refresh", "Aktualisieren", "Actualizar", "Actualiser")

    def _sync_section_visibility(self) -> None:
        for section in ("analyze", "library", "storage", "settings"):
            pane = self.query_one(f"#pane_{section}", Vertical)
            if section == self.current_section:
                pane.add_class("-active")
            else:
                pane.remove_class("-active")
        for panel_id, visible in (
            ("#sidebar_library_panel", self.current_section == "library"),
            ("#sidebar_storage_panel", self.current_section == "storage"),
        ):
            panel = self.query_one(panel_id, Vertical)
            if visible:
                panel.add_class("-active")
            else:
                panel.remove_class("-active")

    def _refresh_analyze(self) -> None:
        jobs = self.context.db.list_jobs(limit=50)
        if jobs and not self.selected_job_id:
            self.selected_job_id = jobs[0].id

        status = self.query_one("#status_line", Static)
        timeline = self.query_one("#timeline", Static)
        segment_table = self.query_one("#segment_table", DataTable)
        segment_table.clear(columns=False)

        if not self.selected_job_id:
            status.update(self.tr("Ready", "Bereit", "Listo", "Prêt"))
            timeline.update(self.tr("No analysis yet", "Noch keine Analyse", "Sin análisis", "Pas d’analyse"))
            self.query_one("#segment_inspector", Static).update(self.tr("Run analysis and pick a segment.", "Analyse starten und Segment wählen.", "Ejecuta un análisis y elige un segmento.", "Lancez une analyse et choisissez un segment."))
            return

        job = self.context.db.get_job(self.selected_job_id)
        if not job:
            status.update(self.tr("Ready", "Bereit", "Listo", "Prêt"))
            timeline.update(self.tr("No analysis yet", "Noch keine Analyse", "Sin análisis", "Pas d’analyse"))
            return

        items = self.context.db.get_source_items(job.id)
        segments = self._filtered_segments(self.context.db.get_segments(job.id))
        status.update(self._job_status_text(job, len(items), len(segments)))
        timeline.update(self._render_timeline(segments))
        for segment in segments:
            segment_table.add_row(
                self._time_range(segment),
                self._kind_label(segment.kind),
                self._segment_title(segment),
                key=self._segment_key(segment),
            )
        if segments:
            selected_id = self.selected_segment_id or self._segment_key(segments[0])
            self.selected_segment_id = selected_id
            self._refresh_segment_inspector()
        else:
            self.selected_segment_id = ""
            self.query_one("#segment_inspector", Static).update(self.tr("No segments match this filter.", "Kein Segment passt zum Filter.", "Ningún segmento coincide.", "Aucun segment ne correspond."))

    def _refresh_segment_inspector(self) -> None:
        inspector = self.query_one("#segment_inspector", Static)
        if not self.selected_job_id or not self.selected_segment_id:
            inspector.update(self.tr("Pick a segment to inspect it.", "Segment für Details wählen.", "Elige un segmento.", "Choisissez un segment."))
            return
        segments = self.context.db.get_segments(self.selected_job_id)
        selected = next((segment for segment in segments if self._segment_key(segment) == self.selected_segment_id), None)
        if not selected:
            inspector.update(self.tr("Pick a segment to inspect it.", "Segment für Details wählen.", "Elige un segmento.", "Choisissez un segment."))
            return

        lines = [
            self._segment_title(selected),
            "",
            f"{self.tr('Range', 'Zeit', 'Rango', 'Plage')}: {self._time_range(selected)}",
            f"{self.tr('Type', 'Typ', 'Tipo', 'Type')}: {self._kind_label(selected.kind)}",
        ]
        if selected.confidence:
            lines.append(f"{self.tr('Confidence', 'Trefferquote', 'Confianza', 'Confiance')}: {selected.confidence:.2f}")
        if selected.providers:
            lines.append(f"{self.tr('Providers', 'Provider', 'Proveedores', 'Fournisseurs')}: {', '.join(provider.value for provider in selected.providers)}")
        if selected.repeat_group_id:
            lines.append(f"{self.tr('Repeat', 'Wiederholt', 'Repite', 'Répété')}: {selected.repeat_group_id}")
        if selected.metadata_hints:
            lines.append(f"{self.tr('Hints', 'Hinweise', 'Pistas', 'Indices')}: {', '.join(selected.metadata_hints[:3])}")
        if selected.track and selected.track.external_links:
            lines.append("")
            lines.append(f"{self.tr('Links', 'Links', 'Enlaces', 'Liens')}:")
            for label, url in selected.track.external_links.items():
                lines.append(f"- {label}: {url}")
        if selected.alternates:
            lines.append("")
            lines.append(f"{self.tr('Alternates', 'Alternativen', 'Alternativas', 'Alternatives')}:")
            for alt in selected.alternates[:4]:
                lines.append(f"- {self._display_track(alt.artist, alt.title)}")
        inspector.update("\n".join(lines))

    def _refresh_library(self) -> None:
        table = self.query_one("#library_table", DataTable)
        table.clear(columns=False)
        entries = self.context.manager.list_library_entries(limit=100)
        for entry in entries:
            table.add_row(
                entry.title,
                entry.status.value,
                str(entry.segment_count),
                self._format_bytes(entry.artifact_size_bytes),
                "yes" if entry.pinned else "",
                key=entry.job_id,
            )
        if entries and not self.selected_job_id:
            self.selected_job_id = entries[0].job_id
        self._refresh_library_detail()

    def _refresh_library_detail(self) -> None:
        detail = self.query_one("#library_detail", Static)
        if not self.selected_job_id:
            detail.update(self.tr("Pick an analysis from the library.", "Analyse aus der Bibliothek wählen.", "Elige un análisis de la biblioteca.", "Choisissez une analyse."))
            return
        job = self.context.db.get_job(self.selected_job_id)
        if not job:
            detail.update(self.tr("Pick an analysis from the library.", "Analyse aus der Bibliothek wählen.", "Elige un análisis de la biblioteca.", "Choisissez une analyse."))
            return
        items = self.context.db.get_source_items(job.id)
        segments = self.context.db.get_segments(job.id)
        pinned = self.context.db.is_job_pinned(job.id)
        lines = [
            f"{self.tr('Status', 'Status', 'Estado', 'État')}: {job.status.value}",
            f"{self.tr('Created', 'Erstellt', 'Creado', 'Créé')}: {job.created_at}",
            f"{self.tr('Pinned', 'Gepinnt', 'Fijado', 'Épinglé')}: {self.tr('yes', 'ja', 'sí', 'oui') if pinned else self.tr('no', 'nein', 'no', 'non')}",
            "",
            f"{self.tr('Inputs', 'Eingaben', 'Entradas', 'Entrées')}:",
        ]
        for item in items:
            lines.append(f"- {item.metadata.title or item.input_value}")
        lines.append("")
        lines.append(f"{self.tr('Preview', 'Vorschau', 'Vista previa', 'Aperçu')}:")
        for segment in segments[:12]:
            lines.append(f"- {self._time_range(segment)} {self._segment_title(segment)}")
        detail.update("\n".join(lines))

    def _refresh_storage_scopes(self) -> None:
        table = self.query_one("#storage_scope_table", DataTable)
        table.clear(columns=False)
        total = self.context.manager.storage_summary()
        table.add_row(self.tr("All temp files", "Alle Temp-Dateien", "Todos los temporales", "Tous les temporaires"), self._format_bytes(total.total_size_bytes), "-", "", key="all")
        entries = self.context.manager.list_library_entries(limit=100)
        for entry in entries:
            table.add_row(
                entry.title,
                self._format_bytes(entry.artifact_size_bytes),
                str(entry.segment_count),
                self.tr("yes", "ja", "sí", "oui") if entry.pinned else "",
                key=entry.job_id,
            )
        if not self.selected_storage_job_id:
            self.selected_storage_job_id = "all"

    def _refresh_storage_detail(self) -> None:
        detail = self.query_one("#storage_detail", Static)
        job_id = None if self.selected_storage_job_id in {"", "all"} else self.selected_storage_job_id
        summary = self.context.manager.storage_summary(job_id)
        lines = [
            f"{self.tr('Policy', 'Regel', 'Política', 'Politique')}: {self.tr('Auto-clean', 'Auto-Clean', 'Auto-limpieza', 'Nettoyage auto') if summary.auto_clean else self.tr('Kept', 'Behalten', 'Conservado', 'Conservé')}",
            f"{self.tr('Size', 'Größe', 'Tamaño', 'Taille')}: {self._format_bytes(summary.total_size_bytes)}",
            "",
            f"{self.tr('Categories', 'Kategorien', 'Categorías', 'Catégories')}:",
        ]
        for category in summary.categories:
            lines.append(
                f"- {category.category.value}: {category.count} entries • {self._format_bytes(category.size_bytes)}"
            )
        if summary.entries:
            lines.append("")
            lines.append(f"{self.tr('Artifacts', 'Artefakte', 'Artefactos', 'Artefacts')}:")
            for entry in summary.entries[:20]:
                pin = f" [{self.tr('pinned', 'gepinnt', 'fijado', 'épinglé')}]" if entry.pinned else ""
                temp = self.tr("temporary", "temporär", "temporal", "temporaire") if entry.temporary else self.tr("persistent", "dauerhaft", "persistente", "persistant")
                lines.append(f"- {entry.label} • {temp}{pin}")
                lines.append(f"  {entry.path}")
        if summary.locations:
            lines.append("")
            lines.append(f"{self.tr('Locations', 'Orte', 'Ubicaciones', 'Emplacements')}:")
            for key, value in summary.locations.items():
                lines.append(f"- {key}: {value}")
        detail.update("\n".join(lines))

    def _refresh_settings(self) -> None:
        settings = self.query_one("#settings_detail", Static)
        lines = [
            self.tr("Diagnostics", "Diagnose", "Diagnóstico", "Diagnostic"),
            "",
            f"Cache: {self.context.settings.cache_dir}",
            f"Data: {self.context.settings.data_dir}",
            f"Config: {self.context.settings.config_dir}",
            f"{self.tr('Auto-clean', 'Auto-Clean', 'Auto-limpieza', 'Nettoyage auto')}: {self.tr('on', 'an', 'activo', 'actif') if not self.context.settings.retain_artifacts else self.tr('off', 'aus', 'apagado', 'désactivé')}",
            "",
            f"{self.tr('Providers', 'Provider', 'Proveedores', 'Fournisseurs')}:",
        ]
        for state in self.context.manager.provider_states():
            availability = self.tr("ready", "bereit", "listo", "prêt") if state.available else self.tr("missing", "fehlt", "faltante", "manquant")
            lines.append(f"- {state.name.value}: {availability}")
            if state.reason:
                lines.append(f"  {state.reason}")
        settings.update("\n".join(lines))

    def _submit_current_input(self) -> None:
        input_widget = self.query_one("#analyze_input", Input)
        value = input_widget.value.strip()
        if not value:
            return
        job = self.context.manager.submit_payload(inputs=[value])
        self.selected_job_id = job.id
        self.current_section = "analyze"
        input_widget.value = ""
        self.refresh_all()

    def _open_selected_in_analyze(self) -> None:
        if not self.selected_job_id:
            return
        self.current_section = "analyze"
        self._refresh_analyze()

    def _toggle_pin_selected(self) -> None:
        if not self.selected_job_id:
            return
        self.context.manager.set_job_pinned(self.selected_job_id, not self.context.db.is_job_pinned(self.selected_job_id))
        self.refresh_all()

    def _cleanup_selected_storage(self) -> None:
        if self.selected_storage_job_id in {"", "all"}:
            self.context.manager.cleanup_temporary_artifacts()
        else:
            self.context.manager.cleanup_job_artifacts(self.selected_storage_job_id)
        self.refresh_all()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        table_id = event.data_table.id
        key = str(event.row_key.value)
        if table_id == "segment_table":
            self.selected_segment_id = key
            self._refresh_segment_inspector()
        elif table_id == "library_table":
            self.selected_job_id = key
            self._refresh_library_detail()
            self._refresh_analyze()
        elif table_id == "storage_scope_table":
            self.selected_storage_job_id = key
            self._refresh_storage_detail()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id == "new_analysis":
            self.action_new_analysis()
        elif button_id == "nav_analyze":
            self.action_show_analyze()
        elif button_id == "nav_library":
            self.action_show_library()
        elif button_id == "nav_storage":
            self.action_show_storage()
        elif button_id == "nav_settings":
            self.action_show_settings()
        elif button_id == "analyze_button":
            self._submit_current_input()
        elif button_id == "library_open":
            self._open_selected_in_analyze()
        elif button_id == "library_pin":
            self._toggle_pin_selected()
        elif button_id == "library_refresh":
            self.refresh_all()
        elif button_id == "storage_cleanup_selected":
            self._cleanup_selected_storage()
        elif button_id == "storage_cleanup_all":
            self.context.manager.cleanup_temporary_artifacts()
            self.refresh_all()
        elif button_id == "storage_pin_toggle":
            if self.selected_storage_job_id not in {"", "all"}:
                self.context.manager.set_job_pinned(
                    self.selected_storage_job_id,
                    not self.context.db.is_job_pinned(self.selected_storage_job_id),
                )
                self.refresh_all()
        elif button_id == "storage_refresh":
            self.refresh_all()
        elif button_id == "toggle_songs":
            self.show_only_songs = not self.show_only_songs
            self._refresh_analyze()
        elif button_id == "choose_file":
            self.notify(self.tr("File picker is in the macOS app.", "Dateiauswahl ist in der macOS-App.", "El selector de archivos está en la app macOS.", "Le sélecteur de fichiers est dans l’app macOS."), severity="information")
        elif button_id == "record_mic":
            self.notify(self.tr("Mic capture is in the macOS app.", "Mikroaufnahme ist in der macOS-App.", "La captura de micrófono está en la app macOS.", "La capture micro est dans l’app macOS."), severity="information")
        elif button_id == "record_system":
            self.notify(self.tr("System audio capture is in the macOS app.", "Systemaudioaufnahme ist in der macOS-App.", "La captura de audio del sistema está en la app macOS.", "La capture audio système est dans l’app macOS."), severity="information")
        elif button_id.startswith("lang_"):
            self.set_language(button_id.split("_", 1)[1])
            self.refresh_all()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "analyze_input":
            self._submit_current_input()

    def action_new_analysis(self) -> None:
        self.current_section = "analyze"
        self.selected_segment_id = ""
        self.query_one("#analyze_input", Input).value = ""
        self.query_one("#status_line", Static).update(self.tr("Ready", "Bereit", "Listo", "Prêt"))

    def action_refresh(self) -> None:
        self.refresh_all()

    def action_show_analyze(self) -> None:
        self.current_section = "analyze"
        self._sync_section_visibility()

    def action_show_library(self) -> None:
        self.current_section = "library"
        self._sync_section_visibility()

    def action_show_storage(self) -> None:
        self.current_section = "storage"
        self._sync_section_visibility()

    def action_show_settings(self) -> None:
        self.current_section = "settings"
        self._sync_section_visibility()

    def _filtered_segments(self, segments: Iterable[DetectedSegment]) -> list[DetectedSegment]:
        items = list(segments)
        if self.show_only_songs:
            matched = [segment for segment in items if segment.kind == SegmentKind.MATCHED_TRACK]
            return matched if matched else items
        return items

    def _job_status_text(self, job, item_count: int, segment_count: int) -> str:
        status = job.status.value if isinstance(job.status, JobStatus) else str(job.status)
        return f"Job {job.id[:8]} • {status} • {item_count} {self.tr('items', 'Einträge', 'elementos', 'éléments')} • {segment_count} {self.tr('visible segments', 'sichtbare Segmente', 'segmentos visibles', 'segments visibles')}"

    def _render_timeline(self, segments: list[DetectedSegment]) -> str:
        if not segments:
            return self.tr("No segments yet", "Noch keine Segmente", "Aún no hay segmentos", "Pas encore de segments")
        total_end = max(segment.end_ms for segment in segments)
        width = 80
        cells = ["·"] * width
        for segment in segments:
            start = int((segment.start_ms / max(1, total_end)) * width)
            end = max(start + 1, int((segment.end_ms / max(1, total_end)) * width))
            fill = self._timeline_fill(segment.kind)
            for index in range(min(width, start), min(width, end)):
                cells[index] = fill
        bar = "".join(cells)
        legend = self.tr("█ song  ▒ unresolved  ░ speech  · silence", "█ Song  ▒ unklar  ░ Sprache  · Stille", "█ canción  ▒ sin resolver  ░ voz  · silencio", "█ titre  ▒ non résolu  ░ voix  · silence")
        return f"{bar}\n{legend}"

    def _timeline_fill(self, kind: SegmentKind) -> str:
        if kind == SegmentKind.MATCHED_TRACK:
            return "█"
        if kind == SegmentKind.MUSIC_UNRESOLVED:
            return "▒"
        if kind == SegmentKind.SPEECH_ONLY:
            return "░"
        return "·"

    def _segment_title(self, segment: DetectedSegment) -> str:
        if segment.track:
            return self._display_track(segment.track.artist, segment.track.title)
        return self._kind_label(segment.kind)

    def _segment_key(self, segment: DetectedSegment) -> str:
        title = segment.track.title if segment.track else segment.kind.value
        return f"{segment.source_item_id}:{segment.start_ms}:{segment.end_ms}:{title}"

    def _kind_label(self, kind: SegmentKind) -> str:
        mapping = {
            SegmentKind.MATCHED_TRACK: self.tr("Song", "Song", "Canción", "Titre"),
            SegmentKind.MUSIC_UNRESOLVED: self.tr("Music", "Musik", "Música", "Musique"),
            SegmentKind.SPEECH_ONLY: self.tr("Speech", "Sprache", "Voz", "Voix"),
            SegmentKind.SILENCE_OR_FX: self.tr("Silence/FX", "Stille/FX", "Silencio/FX", "Silence/FX"),
        }
        return mapping.get(kind, kind.value.replace("_", " "))

    def _display_track(self, artist: str | None, title: str) -> str:
        if artist:
            return f"{artist} - {title}"
        return title

    def _time_range(self, segment: DetectedSegment) -> str:
        return f"{self._format_time(segment.start_ms)}-{self._format_time(segment.end_ms)}"

    def _format_time(self, milliseconds: int) -> str:
        total_seconds = milliseconds // 1000
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:02d}:{seconds:02d}"

    def _format_bytes(self, size_bytes: int) -> str:
        units = ["B", "KB", "MB", "GB"]
        value = float(size_bytes)
        unit = units[0]
        for candidate in units[1:]:
            if value < 1024:
                break
            value /= 1024
            unit = candidate
        if unit == "B":
            return f"{int(value)} {unit}"
        return f"{value:.1f} {unit}"


def launch_tui(context: AppContext) -> None:
    MusicFetchTUI(context).run()
