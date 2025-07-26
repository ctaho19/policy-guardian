from __future__ import annotations

import json
import logging
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from PyQt6.QtCore import QSettings, QThread, QTimer, pyqtSignal, QRect, QMarginsF, Qt
    from PyQt6.QtGui import QAction, QFont, QIcon, QPalette, QColor, QPainter, QPen, QBrush, QTextDocument, QPageSize
    from PyQt6.QtPrintSupport import QPrinter
    from PyQt6.QtWidgets import (
        QApplication, QCheckBox, QComboBox, QFileDialog, QGroupBox,
        QHBoxLayout, QHeaderView, QLabel, QMainWindow, QMessageBox,
        QPushButton, QProgressBar, QTextEdit, QTableWidget, QTableWidgetItem,
        QVBoxLayout, QWidget, QSplitter, QTabWidget, QStatusBar, QLineEdit,
        QFrame, QScrollArea
    )
    
    HAS_QT = True
except ImportError:
    HAS_QT = False


if HAS_QT:
    from policyguardian import PolicyAnalyzer, __version__
    from policyguardian.core.checks import CheckResult, CheckSeverity
    from policyguardian.core.exceptions import PolicyGuardianError


def setup_gui_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("policy_guardian.log"),
            logging.StreamHandler()
        ]
    )


class ThemeManager:
    LIGHT_THEME = {
        'background': '#ffffff',
        'surface': '#f5f5f5',
        'primary': '#2196F3',
        'primary_variant': '#1976D2',
        'secondary': '#03DAC6',
        'text_primary': '#000000',
        'text_secondary': '#666666',
        'border': '#e0e0e0',
        'critical': '#f44336',
        'high': '#ff5722',
        'medium': '#ff9800',
        'low': '#2196f3',
        'success': '#4caf50',
        'warning': '#ffc107'
    }
    
    DARK_THEME = {
        'background': '#121212',
        'surface': '#1e1e1e',
        'primary': '#bb86fc',
        'primary_variant': '#3700b3',
        'secondary': '#03dac6',
        'text_primary': '#ffffff',
        'text_secondary': '#b3b3b3',
        'border': '#333333',
        'critical': '#cf6679',
        'high': '#ff8a65',
        'medium': '#ffb74d',
        'low': '#64b5f6',
        'success': '#81c784',
        'warning': '#ffeb3b'
    }
    
    def __init__(self, app: QApplication):
        self.app = app
        self.current_theme = "light"
        self.themes = {
            "light": self.LIGHT_THEME,
            "dark": self.DARK_THEME
        }
    
    def apply_theme(self, theme_name: str) -> None:
        if theme_name not in self.themes:
            return
            
        self.current_theme = theme_name
        theme = self.themes[theme_name]
        
        palette = QPalette()
        
        # Set colors
        palette.setColor(QPalette.ColorRole.Window, QColor(theme['background']))
        palette.setColor(QPalette.ColorRole.WindowText, QColor(theme['text_primary']))
        palette.setColor(QPalette.ColorRole.Base, QColor(theme['surface']))
        palette.setColor(QPalette.ColorRole.AlternateBase, QColor(theme['border']))
        palette.setColor(QPalette.ColorRole.ToolTipBase, QColor(theme['surface']))
        palette.setColor(QPalette.ColorRole.ToolTipText, QColor(theme['text_primary']))
        palette.setColor(QPalette.ColorRole.Text, QColor(theme['text_primary']))
        palette.setColor(QPalette.ColorRole.Button, QColor(theme['surface']))
        palette.setColor(QPalette.ColorRole.ButtonText, QColor(theme['text_primary']))
        palette.setColor(QPalette.ColorRole.BrightText, QColor(theme['secondary']))
        palette.setColor(QPalette.ColorRole.Link, QColor(theme['primary']))
        palette.setColor(QPalette.ColorRole.Highlight, QColor(theme['primary']))
        palette.setColor(QPalette.ColorRole.HighlightedText, QColor(theme['background']))
        
        self.app.setPalette(palette)
        
        # Apply custom stylesheet
        stylesheet = self.get_stylesheet(theme)
        self.app.setStyleSheet(stylesheet)
    
    def get_stylesheet(self, theme: Dict[str, str]) -> str:
        return f"""
        QMainWindow {{
            background-color: {theme['background']};
            color: {theme['text_primary']};
        }}
        
        QGroupBox {{
            font-weight: bold;
            border: 2px solid {theme['border']};
            border-radius: 5px;
            margin-top: 10px;
            padding-top: 5px;
            background-color: {theme['surface']};
        }}
        
        QGroupBox::title {{
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 5px 0 5px;
            color: {theme['text_primary']};
        }}
        
        QPushButton {{
            background-color: {theme['primary']};
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            font-weight: bold;
            min-width: 80px;
        }}
        
        QPushButton:hover {{
            background-color: {theme['primary_variant']};
        }}
        
        QPushButton:pressed {{
            background-color: {theme['primary_variant']};
        }}
        
        QPushButton:disabled {{
            background-color: {theme['border']};
            color: {theme['text_secondary']};
        }}
        
        QLineEdit {{
            border: 2px solid {theme['border']};
            border-radius: 4px;
            padding: 5px;
            background-color: {theme['surface']};
            color: {theme['text_primary']};
        }}
        
        QLineEdit:focus {{
            border-color: {theme['primary']};
        }}
        
        QTextEdit {{
            border: 2px solid {theme['border']};
            border-radius: 4px;
            background-color: {theme['surface']};
            color: {theme['text_primary']};
        }}
        
        QComboBox {{
            border: 2px solid {theme['border']};
            border-radius: 4px;
            padding: 5px;
            background-color: {theme['surface']};
            color: {theme['text_primary']};
        }}
        
        QComboBox:hover {{
            border-color: {theme['primary']};
        }}
        
        QTableWidget {{
            border: 2px solid {theme['border']};
            background-color: {theme['surface']};
            alternate-background-color: {theme['background']};
            color: {theme['text_primary']};
            gridline-color: {theme['border']};
        }}
        
        QTableWidget::item {{
            padding: 8px;
        }}
        
        QTableWidget::item:selected {{
            background-color: {theme['primary']};
            color: white;
        }}
        
        QHeaderView::section {{
            background-color: {theme['border']};
            color: {theme['text_primary']};
            padding: 8px;
            border: none;
            font-weight: bold;
        }}
        
        QTabWidget::pane {{
            border: 2px solid {theme['border']};
            background-color: {theme['surface']};
        }}
        
        QTabBar::tab {{
            background-color: {theme['border']};
            color: {theme['text_primary']};
            padding: 8px 16px;
            margin-right: 2px;
        }}
        
        QTabBar::tab:selected {{
            background-color: {theme['primary']};
            color: white;
        }}
        
        QTabBar::tab:hover:!selected {{
            background-color: {theme['primary_variant']};
            color: white;
        }}
        
        QProgressBar {{
            border: 2px solid {theme['border']};
            border-radius: 4px;
            text-align: center;
            background-color: {theme['surface']};
        }}
        
        QProgressBar::chunk {{
            background-color: {theme['primary']};
            border-radius: 2px;
        }}
        
        QCheckBox {{
            color: {theme['text_primary']};
        }}
        
        QCheckBox::indicator:checked {{
            background-color: {theme['primary']};
            border: 2px solid {theme['primary']};
        }}
        
        QCheckBox::indicator:unchecked {{
            background-color: {theme['surface']};
            border: 2px solid {theme['border']};
        }}
        
        QLabel {{
            color: {theme['text_primary']};
        }}
        
        QStatusBar {{
            background-color: {theme['surface']};
            color: {theme['text_primary']};
            border-top: 1px solid {theme['border']};
        }}
        """
    
    def get_color(self, color_key: str) -> str:
        return self.themes[self.current_theme].get(color_key, '#000000')


class SeverityChartWidget(QWidget):
    def __init__(self, theme_manager: ThemeManager):
        super().__init__()
        self.theme_manager = theme_manager
        self.severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        self.setMinimumSize(300, 200)
        
    def update_data(self, severity_counts: Dict[str, int]):
        self.severity_counts = severity_counts
        self.update()
        
    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Get widget dimensions
        width = self.width()
        height = self.height()
        
        # Calculate total issues
        total = sum(self.severity_counts.values())
        if total == 0:
            painter.drawText(width // 2 - 50, height // 2, "No issues found")
            return
        
        # Define colors for each severity
        colors = {
            "critical": QColor(self.theme_manager.get_color("critical")),
            "high": QColor(self.theme_manager.get_color("high")),
            "medium": QColor(self.theme_manager.get_color("medium")),
            "low": QColor(self.theme_manager.get_color("low"))
        }
        
        # Draw pie chart
        chart_rect = QRect(20, 20, min(width - 40, height - 80), min(width - 40, height - 80))
        start_angle = 0
        
        for severity, count in self.severity_counts.items():
            if count > 0:
                angle = int((count / total) * 360 * 16)  # Qt uses 16ths of a degree
                painter.setBrush(QBrush(colors[severity]))
                painter.setPen(QPen(QColor(self.theme_manager.get_color("border")), 2))
                painter.drawPie(chart_rect, start_angle, angle)
                start_angle += angle
        
        # Draw legend
        legend_y = chart_rect.bottom() + 20
        legend_x = 20
        
        for i, (severity, count) in enumerate(self.severity_counts.items()):
            if count > 0:
                # Draw color box
                color_rect = QRect(legend_x, legend_y + i * 25, 15, 15)
                painter.fillRect(color_rect, colors[severity])
                painter.setPen(QPen(QColor(self.theme_manager.get_color("border")), 1))
                painter.drawRect(color_rect)
                
                # Draw text
                painter.setPen(QPen(QColor(self.theme_manager.get_color("text_primary"))))
                text = f"{severity.capitalize()}: {count} ({count/total*100:.1f}%)"
                painter.drawText(legend_x + 25, legend_y + i * 25 + 12, text)


class AnalysisWorker(QThread):
    
    progress = pyqtSignal(int)  # Progress percentage
    status = pyqtSignal(str)   # Status message
    result = pyqtSignal(dict)  # Analysis results
    error = pyqtSignal(str)    # Error message
    
    def __init__(self, files: List[Path], check_names: Optional[List[str]] = None):
        super().__init__()
        self.files = files
        self.check_names = check_names
        self.analyzer = PolicyAnalyzer()
        
    def run(self) -> None:
        try:
            total_files = len(self.files)
            all_results = {}
            
            for i, file_path in enumerate(self.files):
                progress_percent = int((i / total_files) * 100)
                self.progress.emit(progress_percent)
                self.status.emit(f"Analyzing {file_path.name}...")
                
                results = self.analyzer.analyze_file(
                    file_path,
                    check_names=self.check_names,
                    use_iterative_parsing=True  # Use memory-efficient parsing
                )
                
                all_results[str(file_path)] = results
                
            self.progress.emit(100)
            self.status.emit("Analysis complete")
            self.result.emit(all_results)
            
        except Exception as e:
            self.error.emit(str(e))


class PolicyGuardianGUI(QMainWindow):
    
    def __init__(self, app: QApplication):
        super().__init__()
        if not HAS_QT:
            raise ImportError("PyQt6 is required for GUI functionality")
            
        self.app = app
        self.settings = QSettings("PolicyGuardian", "PolicyGuardian")
        self.analyzer = PolicyAnalyzer()
        self.current_results: Dict[str, List[CheckResult]] = {}
        self.filtered_results: List[tuple] = []  # Store (file_path, result) tuples for current filter
        
        # Initialize theme manager
        self.theme_manager = ThemeManager(app)
        saved_theme = self.settings.value("theme", "light")
        self.theme_manager.apply_theme(saved_theme)
        
        self.init_ui()
        self.restore_settings()
        
        setup_gui_logging()
        self.logger = logging.getLogger(__name__)
        
    def init_ui(self) -> None:
        self.setWindowTitle(f"Policy Guardian v{__version__}")
        self.setGeometry(100, 100, 1200, 800)
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_splitter = QSplitter()
        central_widget.setLayout(QVBoxLayout())
        central_widget.layout().addWidget(main_splitter)
        
        control_panel = self.create_control_panel()
        main_splitter.addWidget(control_panel)
        
        results_panel = self.create_results_panel()
        main_splitter.addWidget(results_panel)
        
        main_splitter.setSizes([400, 800])
        
        self.create_status_bar()
        
        self.create_menu_bar()
        
    def create_control_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        file_group = QGroupBox("Configuration Files")
        file_layout = QVBoxLayout(file_group)
        
        self.files_list = QTextEdit()
        self.files_list.setMaximumHeight(100)
        self.files_list.setPlaceholderText("No files selected...")
        file_layout.addWidget(self.files_list)
        
        file_buttons_layout = QHBoxLayout()
        
        self.select_files_btn = QPushButton("📁 Select Files")
        self.select_files_btn.clicked.connect(self.select_files)
        file_buttons_layout.addWidget(self.select_files_btn)
        
        self.clear_files_btn = QPushButton("🗑️ Clear")
        self.clear_files_btn.clicked.connect(self.clear_files)
        file_buttons_layout.addWidget(self.clear_files_btn)
        
        file_layout.addLayout(file_buttons_layout)
        layout.addWidget(file_group)
        
        checks_group = QGroupBox("Security Checks")
        checks_layout = QVBoxLayout(checks_group)
        
        check_buttons_layout = QHBoxLayout()
        
        self.select_all_btn = QPushButton("Select All")
        self.select_all_btn.clicked.connect(self.select_all_checks)
        check_buttons_layout.addWidget(self.select_all_btn)
        
        self.select_none_btn = QPushButton("Select None")
        self.select_none_btn.clicked.connect(self.select_no_checks)
        check_buttons_layout.addWidget(self.select_none_btn)
        
        checks_layout.addLayout(check_buttons_layout)
        
        self.check_boxes = {}
        available_checks = self.analyzer.get_available_checks()
        
        for check_id, description in available_checks.items():
            checkbox = QCheckBox(check_id)
            checkbox.setToolTip(description)
            checkbox.setChecked(True)
            self.check_boxes[check_id] = checkbox
            checks_layout.addWidget(checkbox)
            
        layout.addWidget(checks_group)
        
        analysis_group = QGroupBox("Analysis")
        analysis_layout = QVBoxLayout(analysis_group)
        
        self.run_analysis_btn = QPushButton("🔍 Run Analysis")
        self.run_analysis_btn.clicked.connect(self.run_analysis)
        analysis_layout.addWidget(self.run_analysis_btn)
        
        # Progress container
        self.progress_container = QWidget()
        progress_container_layout = QVBoxLayout(self.progress_container)
        progress_container_layout.setContentsMargins(0, 0, 0, 0)
        
        self.progress_label = QLabel("Ready to analyze")
        self.progress_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        progress_container_layout.addWidget(self.progress_label)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(True)
        progress_container_layout.addWidget(self.progress_bar)
        
        analysis_layout.addWidget(self.progress_container)
        
        layout.addWidget(analysis_group)
        
        export_group = QGroupBox("Export Results")
        export_layout = QVBoxLayout(export_group)
        
        self.export_json_btn = QPushButton("📄 Export JSON")
        self.export_json_btn.clicked.connect(self.export_json)
        self.export_json_btn.setEnabled(False)
        export_layout.addWidget(self.export_json_btn)
        
        self.export_pdf_btn = QPushButton("📊 Export PDF")
        self.export_pdf_btn.clicked.connect(self.export_pdf)
        self.export_pdf_btn.setEnabled(False)
        export_layout.addWidget(self.export_pdf_btn)
        
        layout.addWidget(export_group)
        
        layout.addStretch()
        
        return panel
        
    def create_results_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        header_layout = QVBoxLayout()
        
        # Title row
        title_layout = QHBoxLayout()
        self.results_label = QLabel("📊 Analysis Results")
        self.results_label.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        title_layout.addWidget(self.results_label)
        title_layout.addStretch()
        header_layout.addLayout(title_layout)
        
        # Filters row
        filters_layout = QHBoxLayout()
        
        # Search box
        search_label = QLabel("🔍 Search:")
        filters_layout.addWidget(search_label)
        
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Search rules, checks, or messages...")
        self.search_box.textChanged.connect(self.filter_results)
        self.search_box.setMinimumWidth(200)
        filters_layout.addWidget(self.search_box)
        
        filters_layout.addStretch()
        
        # Severity filter
        severity_label = QLabel("⚠️ Severity:")
        filters_layout.addWidget(severity_label)
        
        self.severity_filter = QComboBox()
        self.severity_filter.addItems(["All", "Critical", "High", "Medium", "Low"])
        self.severity_filter.currentTextChanged.connect(self.filter_results)
        self.severity_filter.setMinimumWidth(100)
        filters_layout.addWidget(self.severity_filter)
        
        # Check type filter
        check_label = QLabel("🔧 Check:")
        filters_layout.addWidget(check_label)
        
        self.check_filter = QComboBox()
        self.check_filter.addItem("All")
        self.check_filter.currentTextChanged.connect(self.filter_results)
        self.check_filter.setMinimumWidth(150)
        filters_layout.addWidget(self.check_filter)
        
        # Clear filters button
        self.clear_filters_btn = QPushButton("🗑️ Clear Filters")
        self.clear_filters_btn.clicked.connect(self.clear_filters)
        filters_layout.addWidget(self.clear_filters_btn)
        
        header_layout.addLayout(filters_layout)
        layout.addLayout(header_layout)
        
        self.results_tabs = QTabWidget()
        layout.addWidget(self.results_tabs)
        
        # Summary tab with text and chart
        summary_widget = QWidget()
        summary_layout = QHBoxLayout(summary_widget)
        
        self.summary_text = QTextEdit()
        self.summary_text.setReadOnly(True)
        summary_layout.addWidget(self.summary_text)
        
        # Add chart widget
        self.severity_chart = SeverityChartWidget(self.theme_manager)
        summary_layout.addWidget(self.severity_chart)
        
        self.results_tabs.addTab(summary_widget, "📊 Summary")
        
        # Create detailed results with splitter for detail panel
        detailed_widget = QWidget()
        detailed_layout = QHBoxLayout(detailed_widget)
        
        # Results table on the left
        self.results_table = QTableWidget()
        self.setup_results_table()
        detailed_layout.addWidget(self.results_table, 2)  # 2/3 of the space
        
        # Detail panel on the right
        self.detail_panel = self.create_detail_panel()
        detailed_layout.addWidget(self.detail_panel, 1)  # 1/3 of the space
        
        self.results_tabs.addTab(detailed_widget, "📋 Detailed Results")
        
        return panel
    
    def create_detail_panel(self) -> QWidget:
        panel = QGroupBox("🔍 Issue Details")
        layout = QVBoxLayout(panel)
        
        # Issue info section
        info_group = QGroupBox("📝 Issue Information")
        info_layout = QVBoxLayout(info_group)
        
        self.detail_rule_label = QLabel("Rule: (No selection)")
        self.detail_rule_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        info_layout.addWidget(self.detail_rule_label)
        
        self.detail_check_label = QLabel("Check: (No selection)")
        info_layout.addWidget(self.detail_check_label)
        
        self.detail_severity_label = QLabel("Severity: (No selection)")
        info_layout.addWidget(self.detail_severity_label)
        
        self.detail_file_label = QLabel("File: (No selection)")
        info_layout.addWidget(self.detail_file_label)
        
        layout.addWidget(info_group)
        
        # Message section
        message_group = QGroupBox("💬 Description")
        message_layout = QVBoxLayout(message_group)
        
        self.detail_message = QTextEdit()
        self.detail_message.setReadOnly(True)
        self.detail_message.setMaximumHeight(100)
        self.detail_message.setPlainText("Select an issue to view details...")
        message_layout.addWidget(self.detail_message)
        
        layout.addWidget(message_group)
        
        # Technical details section
        details_group = QGroupBox("🔧 Technical Details")
        details_layout = QVBoxLayout(details_group)
        
        self.detail_technical = QTextEdit()
        self.detail_technical.setReadOnly(True)
        self.detail_technical.setPlainText("Select an issue to view technical details...")
        details_layout.addWidget(self.detail_technical)
        
        layout.addWidget(details_group)
        
        # Recommendations section
        recommendations_group = QGroupBox("💡 Recommendations")
        recommendations_layout = QVBoxLayout(recommendations_group)
        
        self.detail_recommendations = QTextEdit()
        self.detail_recommendations.setReadOnly(True)
        self.detail_recommendations.setMaximumHeight(80)
        self.detail_recommendations.setPlainText("Select an issue to view recommendations...")
        recommendations_layout.addWidget(self.detail_recommendations)
        
        layout.addWidget(recommendations_group)
        
        layout.addStretch()
        
        return panel
        
    def setup_results_table(self) -> None:
        headers = ["File", "Rule", "Check", "Severity", "Message"]
        self.results_table.setColumnCount(len(headers))
        self.results_table.setHorizontalHeaderLabels(headers)
        
        header = self.results_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)
        
        self.results_table.setAlternatingRowColors(True)
        self.results_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        
        # Connect row selection to detail panel
        self.results_table.itemSelectionChanged.connect(self.on_result_selected)
        
    def create_status_bar(self) -> None:
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")
        
    def create_menu_bar(self) -> None:
        menubar = self.menuBar()
        
        file_menu = menubar.addMenu("📁 File")
        
        open_action = QAction("📂 Open Configuration Files", self)
        open_action.setShortcut("Ctrl+O")
        open_action.triggered.connect(self.select_files)
        file_menu.addAction(open_action)
        
        file_menu.addSeparator()
        
        exit_action = QAction("🚪 Exit", self)
        exit_action.setShortcut("Ctrl+Q")
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        view_menu = menubar.addMenu("🎨 View")
        
        theme_light_action = QAction("☀️ Light Theme", self)
        theme_light_action.triggered.connect(lambda: self.switch_theme("light"))
        view_menu.addAction(theme_light_action)
        
        theme_dark_action = QAction("🌙 Dark Theme", self)
        theme_dark_action.triggered.connect(lambda: self.switch_theme("dark"))
        view_menu.addAction(theme_dark_action)
        
        help_menu = menubar.addMenu("❓ Help")
        
        about_action = QAction("ℹ️ About", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
        
    def select_files(self) -> None:
        last_dir = self.settings.value("last_directory", str(Path.home()))
        
        files, _ = QFileDialog.getOpenFileNames(
            self,
            "Select Palo Alto Configuration Files",
            last_dir,
            "XML files (*.xml);;All files (*.*)"
        )
        
        if files:
            self.settings.setValue("last_directory", str(Path(files[0]).parent))
            
            file_list = "\n".join(files)
            self.files_list.setPlainText(file_list)
            
            self.status_bar.showMessage(f"Selected {len(files)} file(s)")
            
    def clear_files(self) -> None:
        self.files_list.clear()
        self.status_bar.showMessage("Files cleared")
    
    def switch_theme(self, theme_name: str) -> None:
        self.theme_manager.apply_theme(theme_name)
        self.settings.setValue("theme", theme_name)
        if self.current_results:
            self.update_results_table()
            self.severity_chart.update()  # Redraw chart with new theme
        self.status_bar.showMessage(f"Switched to {theme_name} theme")
        
    def select_all_checks(self) -> None:
        for checkbox in self.check_boxes.values():
            checkbox.setChecked(True)
            
    def select_no_checks(self) -> None:
        for checkbox in self.check_boxes.values():
            checkbox.setChecked(False)
            
    def get_selected_files(self) -> List[Path]:
        file_text = self.files_list.toPlainText().strip()
        if not file_text:
            return []
        
        files = []
        for line in file_text.split("\n"):
            if line.strip():
                files.append(Path(line.strip()))
        
        return files
        
    def get_selected_checks(self) -> Optional[List[str]]:
        selected = []
        for check_id, checkbox in self.check_boxes.items():
            if checkbox.isChecked():
                selected.append(check_id)
        
        return selected if selected else None
        
    def run_analysis(self) -> None:
        files = self.get_selected_files()
        if not files:
            QMessageBox.warning(
                self, 
                "No Files Selected", 
                "Please select at least one configuration file to analyze."
            )
            return
            
        missing_files = [f for f in files if not f.exists()]
        if missing_files:
            QMessageBox.warning(
                self,
                "Missing Files",
                f"The following files do not exist:\n" + "\n".join(map(str, missing_files))
            )
            return
            
        check_names = self.get_selected_checks()
        if not check_names:
            QMessageBox.warning(
                self,
                "No Checks Selected",
                "Please select at least one security check to run."
            )
            return
            
        self.run_analysis_btn.setEnabled(False)
        self.run_analysis_btn.setText("⏳ Analyzing...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.progress_label.setText("Starting analysis...")
        
        self.worker = AnalysisWorker(files, check_names)
        self.worker.progress.connect(self.update_progress)
        self.worker.status.connect(self.update_analysis_status)
        self.worker.result.connect(self.on_analysis_complete)
        self.worker.error.connect(self.on_analysis_error)
        self.worker.start()
    
    def update_progress(self, value: int) -> None:
        self.progress_bar.setValue(value)
    
    def update_analysis_status(self, status: str) -> None:
        self.progress_label.setText(status)
        self.status_bar.showMessage(status)
        
    def on_analysis_complete(self, results: Dict[str, List[CheckResult]]) -> None:
        self.current_results = results
        self.display_results()
        
        # Re-enable UI
        self.run_analysis_btn.setEnabled(True)
        self.run_analysis_btn.setText("🔍 Run Analysis")
        self.progress_bar.setVisible(False)
        self.progress_label.setText("Analysis complete!")
        self.export_json_btn.setEnabled(True)
        self.export_pdf_btn.setEnabled(True)
        
        total_issues = sum(len(file_results) for file_results in results.values())
        self.status_bar.showMessage(f"✅ Analysis complete - Found {total_issues} issues")
        
    def on_analysis_error(self, error_msg: str) -> None:
        QMessageBox.critical(
            self,
            "Analysis Error",
            f"An error occurred during analysis:\n{error_msg}"
        )
        
        # Re-enable UI
        self.run_analysis_btn.setEnabled(True)
        self.run_analysis_btn.setText("🔍 Run Analysis")
        self.progress_bar.setVisible(False)
        self.progress_label.setText("Analysis failed")
        self.status_bar.showMessage("❌ Analysis failed")
        
    def display_results(self) -> None:
        if not self.current_results:
            return
            
        # Populate check filter dropdown
        check_types = set()
        for file_results in self.current_results.values():
            for result in file_results:
                check_types.add(result.check_name)
        
        # Update check filter
        current_check = self.check_filter.currentText()
        self.check_filter.clear()
        self.check_filter.addItem("All")
        for check_type in sorted(check_types):
            self.check_filter.addItem(check_type)
        
        # Restore selection if it still exists
        if current_check in [self.check_filter.itemText(i) for i in range(self.check_filter.count())]:
            self.check_filter.setCurrentText(current_check)
            
        self.update_summary()
        self.update_results_table()
        
    def update_summary(self) -> None:
        total_files = len(self.current_results)
        total_issues = sum(len(file_results) for file_results in self.current_results.values())
        
        severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        for file_results in self.current_results.values():
            for result in file_results:
                severity_counts[result.severity.value] += 1
        
        summary = f"""
Analysis Summary
================

Files analyzed: {total_files}
Total issues found: {total_issues}

Issues by severity:
• Critical: {severity_counts['critical']}
• High: {severity_counts['high']}
• Medium: {severity_counts['medium']}
• Low: {severity_counts['low']}

Files with issues:
"""
        
        for file_path, results in self.current_results.items():
            if results:
                summary += f"\n• {Path(file_path).name}: {len(results)} issues"
            else:
                summary += f"\n• {Path(file_path).name}: ✅ No issues"
        
        self.summary_text.setPlainText(summary)
        
        # Update chart
        self.severity_chart.update_data(severity_counts)
        
    def update_results_table(self) -> None:
        all_results = []
        for file_path, results in self.current_results.items():
            for result in results:
                all_results.append((file_path, result))
        
        # Apply search filter
        search_text = self.search_box.text().lower().strip()
        if search_text:
            all_results = [
                (fp, r) for fp, r in all_results 
                if (search_text in r.rule_name.lower() or 
                    search_text in r.check_name.lower() or 
                    search_text in r.message.lower() or
                    search_text in Path(fp).name.lower())
            ]
        
        # Apply severity filter
        severity_filter = self.severity_filter.currentText()
        if severity_filter != "All":
            filter_severity = severity_filter.lower()
            all_results = [
                (fp, r) for fp, r in all_results 
                if r.severity.value == filter_severity
            ]
        
        # Apply check type filter
        check_filter = self.check_filter.currentText()
        if check_filter != "All":
            all_results = [
                (fp, r) for fp, r in all_results 
                if r.check_name == check_filter
            ]
        
        # Store filtered results for detail panel access
        self.filtered_results = all_results
        
        self.results_table.setRowCount(len(all_results))
        
        for row, (file_path, result) in enumerate(all_results):
            file_item = QTableWidgetItem(Path(file_path).name)
            file_item.setToolTip(file_path)
            self.results_table.setItem(row, 0, file_item)
            
            rule_item = QTableWidgetItem(result.rule_name)
            self.results_table.setItem(row, 1, rule_item)
            
            check_item = QTableWidgetItem(result.check_name)
            self.results_table.setItem(row, 2, check_item)
            
            severity_item = QTableWidgetItem(result.severity.value.upper())
            
            # Use theme-aware severity colors
            severity_color = QColor(self.theme_manager.get_color(result.severity.value))
            severity_item.setForeground(severity_color)
            
            self.results_table.setItem(row, 3, severity_item)
            
            message_item = QTableWidgetItem(result.message)
            message_item.setToolTip(result.message)
            self.results_table.setItem(row, 4, message_item)
    
    def on_result_selected(self) -> None:
        current_row = self.results_table.currentRow()
        if current_row >= 0 and current_row < len(self.filtered_results):
            file_path, result = self.filtered_results[current_row]
            self.update_detail_panel(file_path, result)
        else:
            self.clear_detail_panel()
    
    def update_detail_panel(self, file_path: str, result: CheckResult) -> None:
        # Update basic info
        self.detail_rule_label.setText(f"Rule: {result.rule_name}")
        self.detail_check_label.setText(f"Check: {result.check_name}")
        
        # Set severity with color
        severity_color = self.theme_manager.get_color(result.severity.value)
        self.detail_severity_label.setText(f"Severity: {result.severity.value.upper()}")
        self.detail_severity_label.setStyleSheet(f"color: {severity_color}; font-weight: bold;")
        
        self.detail_file_label.setText(f"File: {Path(file_path).name}")
        self.detail_file_label.setToolTip(file_path)
        
        # Update message
        self.detail_message.setPlainText(result.message)
        
        # Update technical details
        technical_details = self.format_technical_details(result)
        self.detail_technical.setPlainText(technical_details)
        
        # Update recommendations
        recommendations = self.get_recommendations(result)
        self.detail_recommendations.setPlainText(recommendations)
    
    def clear_detail_panel(self) -> None:
        self.detail_rule_label.setText("Rule: (No selection)")
        self.detail_check_label.setText("Check: (No selection)")
        self.detail_severity_label.setText("Severity: (No selection)")
        self.detail_severity_label.setStyleSheet("")
        self.detail_file_label.setText("File: (No selection)")
        self.detail_message.setPlainText("Select an issue to view details...")
        self.detail_technical.setPlainText("Select an issue to view technical details...")
        self.detail_recommendations.setPlainText("Select an issue to view recommendations...")
    
    def format_technical_details(self, result: CheckResult) -> str:
        details = f"Check Type: {result.check_name}\n"
        details += f"Rule Name: {result.rule_name}\n"
        details += f"Severity Level: {result.severity.value}\n\n"
        
        if result.details:
            details += "Additional Details:\n"
            for key, value in result.details.items():
                if isinstance(value, list):
                    details += f"• {key}: {', '.join(map(str, value))}\n"
                elif isinstance(value, dict):
                    details += f"• {key}:\n"
                    for sub_key, sub_value in value.items():
                        details += f"  - {sub_key}: {sub_value}\n"
                else:
                    details += f"• {key}: {value}\n"
        
        if result.line_number:
            details += f"\nLine Number: {result.line_number}"
            
        return details
    
    def get_recommendations(self, result: CheckResult) -> str:
        # Define recommendations based on check type
        recommendations_map = {
            "Overly Permissive Rules": "Consider restricting source, destination, or service parameters to follow the principle of least privilege.",
            "Redundant Rules": "Remove duplicate entries or consolidate similar rules to improve policy clarity and performance.",
            "Zone Based Checks": "Ensure proper zone configuration and avoid overly broad zone assignments.",
            "Rules Missing Security Profiles": "Configure appropriate security profiles (antivirus, anti-spyware, vulnerability protection) for allow rules.",
            "Shadowing Rules": "Reorder rules or modify criteria to ensure intended rule processing order."
        }
        
        base_recommendation = recommendations_map.get(result.check_name, 
            "Review this issue and consider appropriate remediation based on your security policies.")
        
        general_recommendations = """
General Security Best Practices:
• Follow the principle of least privilege
• Regularly review and audit firewall rules
• Document rule purposes and business justifications
• Test rule changes in a controlled environment
• Monitor rule usage and effectiveness"""
        
        return f"{base_recommendation}\n\n{general_recommendations}"
            
    def filter_results(self) -> None:
        if self.current_results:
            self.update_results_table()
    
    def clear_filters(self) -> None:
        self.search_box.clear()
        self.severity_filter.setCurrentText("All")
        self.check_filter.setCurrentText("All")
        if self.current_results:
            self.update_results_table()
            
    def export_json(self) -> None:
        if not self.current_results:
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Results as JSON",
            "policy_guardian_results.json",
            "JSON files (*.json);;All files (*.*)"
        )
        
        if file_path:
            try:
                export_data = {}
                for file_name, results in self.current_results.items():
                    export_data[file_name] = [result.to_dict() for result in results]
                
                with open(file_path, 'w') as f:
                    json.dump(export_data, f, indent=2)
                
                QMessageBox.information(
                    self,
                    "Export Complete",
                    f"Results exported to {file_path}"
                )
                
            except Exception as e:
                QMessageBox.critical(
                    self,
                    "Export Error",
                    f"Failed to export results:\n{str(e)}"
                )
                
    def export_pdf(self) -> None:
        if not self.current_results:
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Report as PDF",
            "policy_guardian_report.pdf",
            "PDF files (*.pdf);;All files (*.*)"
        )
        
        if file_path:
            try:
                self.generate_pdf_report(file_path)
                QMessageBox.information(
                    self,
                    "Export Complete",
                    f"PDF report exported to {file_path}"
                )
                
            except Exception as e:
                QMessageBox.critical(
                    self,
                    "Export Error",
                    f"Failed to export PDF report:\n{str(e)}"
                )
    
    def generate_pdf_report(self, file_path: str) -> None:
        # Set up printer for PDF output
        printer = QPrinter(QPrinter.PrinterMode.HighResolution)
        printer.setOutputFormat(QPrinter.OutputFormat.PdfFormat)
        printer.setOutputFileName(file_path)
        printer.setPageSize(QPageSize(QPageSize.PageSizeId.A4))
        printer.setPageMargins(QMarginsF(20, 20, 20, 20), QPrinter.Unit.Millimeter)
        
        # Create HTML content for the report
        html_content = self.generate_report_html()
        
        # Create QTextDocument and print to PDF
        document = QTextDocument()
        document.setHtml(html_content)
        document.print(printer)
    
    def generate_report_html(self) -> str:
        from datetime import datetime
        
        # Calculate statistics
        total_files = len(self.current_results)
        total_issues = sum(len(file_results) for file_results in self.current_results.values())
        
        severity_counts = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        check_counts = {}
        
        for file_results in self.current_results.values():
            for result in file_results:
                severity_counts[result.severity.value] += 1
                check_counts[result.check_name] = check_counts.get(result.check_name, 0) + 1
        
        # Start building HTML
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Policy Guardian Analysis Report</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 20px;
                    color: #333;
                }}
                
                .header {{
                    text-align: center;
                    border-bottom: 3px solid #2196F3;
                    padding-bottom: 20px;
                    margin-bottom: 30px;
                }}
                
                .header h1 {{
                    color: #2196F3;
                    margin: 0;
                    font-size: 28px;
                }}
                
                .header .subtitle {{
                    color: #666;
                    font-size: 14px;
                    margin-top: 5px;
                }}
                
                .summary-grid {{
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                
                .summary-box {{
                    border: 1px solid #ddd;
                    border-radius: 8px;
                    padding: 20px;
                    background: #f9f9f9;
                }}
                
                .summary-box h3 {{
                    margin-top: 0;
                    color: #2196F3;
                    border-bottom: 1px solid #ddd;
                    padding-bottom: 10px;
                }}
                
                .stat-row {{
                    display: flex;
                    justify-content: space-between;
                    margin: 8px 0;
                    padding: 5px 0;
                }}
                
                .stat-label {{
                    font-weight: bold;
                }}
                
                .severity-critical {{ color: #f44336; }}
                .severity-high {{ color: #ff5722; }}
                .severity-medium {{ color: #ff9800; }}
                .severity-low {{ color: #2196f3; }}
                
                .results-table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 20px;
                    font-size: 12px;
                }}
                
                .results-table th,
                .results-table td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    text-align: left;
                }}
                
                .results-table th {{
                    background-color: #2196F3;
                    color: white;
                    font-weight: bold;
                }}
                
                .results-table tr:nth-child(even) {{
                    background-color: #f9f9f9;
                }}
                
                .file-section {{
                    margin-bottom: 30px;
                    break-inside: avoid;
                }}
                
                .file-header {{
                    background: #e3f2fd;
                    padding: 10px;
                    border-left: 4px solid #2196F3;
                    margin-bottom: 10px;
                }}
                
                .recommendations {{
                    background: #f0f8f0;
                    border: 1px solid #4caf50;
                    border-radius: 5px;
                    padding: 15px;
                    margin-top: 30px;
                }}
                
                .recommendations h3 {{
                    color: #4caf50;
                    margin-top: 0;
                }}
                
                @media print {{
                    body {{ margin: 0; }}
                    .break-page {{ page-break-before: always; }}
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🛡️ Policy Guardian Analysis Report</h1>
                <div class="subtitle">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
            </div>
            
            <div class="summary-grid">
                <div class="summary-box">
                    <h3>📊 Analysis Summary</h3>
                    <div class="stat-row">
                        <span class="stat-label">Files Analyzed:</span>
                        <span>{total_files}</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label">Total Issues:</span>
                        <span>{total_issues}</span>
                    </div>
                </div>
                
                <div class="summary-box">
                    <h3>⚠️ Issues by Severity</h3>
                    <div class="stat-row">
                        <span class="stat-label severity-critical">Critical:</span>
                        <span>{severity_counts['critical']}</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label severity-high">High:</span>
                        <span>{severity_counts['high']}</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label severity-medium">Medium:</span>
                        <span>{severity_counts['medium']}</span>
                    </div>
                    <div class="stat-row">
                        <span class="stat-label severity-low">Low:</span>
                        <span>{severity_counts['low']}</span>
                    </div>
                </div>
            </div>
        """
        
        # Add check type distribution if there are multiple types
        if len(check_counts) > 1:
            html += """
            <div class="summary-box" style="margin-bottom: 30px;">
                <h3>🔧 Issues by Check Type</h3>
            """
            for check_name, count in sorted(check_counts.items()):
                html += f"""
                <div class="stat-row">
                    <span class="stat-label">{check_name}:</span>
                    <span>{count}</span>
                </div>
                """
            html += "</div>"
        
        # Add detailed results for each file
        html += '<div class="break-page"></div><h2>📋 Detailed Results</h2>'
        
        for file_path, results in self.current_results.items():
            file_name = Path(file_path).name
            
            html += f'''
            <div class="file-section">
                <div class="file-header">
                    <h3>📄 {file_name}</h3>
                    <p>Issues found: {len(results)}</p>
                </div>
            '''
            
            if results:
                html += '''
                <table class="results-table">
                    <thead>
                        <tr>
                            <th>Rule</th>
                            <th>Check</th>
                            <th>Severity</th>
                            <th>Description</th>
                        </tr>
                    </thead>
                    <tbody>
                '''
                
                for result in results:
                    severity_class = f"severity-{result.severity.value}"
                    html += f'''
                    <tr>
                        <td>{result.rule_name}</td>
                        <td>{result.check_name}</td>
                        <td class="{severity_class}">{result.severity.value.upper()}</td>
                        <td>{result.message}</td>
                    </tr>
                    '''
                
                html += '''
                    </tbody>
                </table>
                '''
            else:
                html += '<p>✅ No issues found in this file.</p>'
            
            html += '</div>'
        
        # Add recommendations section
        html += '''
        <div class="break-page"></div>
        <div class="recommendations">
            <h3>💡 General Recommendations</h3>
            <ul>
                <li><strong>Follow the Principle of Least Privilege:</strong> Ensure rules only allow the minimum access necessary.</li>
                <li><strong>Regular Review and Audit:</strong> Periodically review firewall rules to remove unnecessary or outdated entries.</li>
                <li><strong>Proper Documentation:</strong> Document the business justification for each rule.</li>
                <li><strong>Security Profiles:</strong> Ensure all allow rules have appropriate security profiles configured.</li>
                <li><strong>Rule Ordering:</strong> Pay attention to rule order to prevent shadowing of important security rules.</li>
                <li><strong>Zone Configuration:</strong> Use specific zones rather than 'any' where possible.</li>
                <li><strong>Testing:</strong> Test rule changes in a controlled environment before deploying to production.</li>
                <li><strong>Monitoring:</strong> Monitor rule usage and effectiveness regularly.</li>
            </ul>
            
            <h4>🚨 Priority Actions</h4>
        '''
        
        # Add priority recommendations based on severity
        if severity_counts['critical'] > 0:
            html += f"<p><strong>Critical Priority:</strong> Address {severity_counts['critical']} critical severity issues immediately.</p>"
        if severity_counts['high'] > 0:
            html += f"<p><strong>High Priority:</strong> Review and remediate {severity_counts['high']} high severity issues.</p>"
        if severity_counts['medium'] > 0:
            html += f"<p><strong>Medium Priority:</strong> Plan remediation for {severity_counts['medium']} medium severity issues.</p>"
        
        html += '''
        </div>
        
        <div style="margin-top: 40px; text-align: center; color: #666; font-size: 12px;">
            <p>Report generated by Policy Guardian v''' + __version__ + '''</p>
            <p>For more information, visit the project documentation.</p>
        </div>
        
        </body>
        </html>
        '''
        
        return html
        
    def show_about(self) -> None:
        QMessageBox.about(
            self,
            "About Policy Guardian",
            f"""
<h2>Policy Guardian v{__version__}</h2>
<p>A comprehensive firewall policy analyzer for Palo Alto Networks configurations.</p>
<p>Developed by: ctaho19</p>
<p>License: MIT</p>
<p><a href="https://github.com/ctaho19/policy-guardian">GitHub Repository</a></p>
            """
        )
        
    def restore_settings(self) -> None:
        geometry = self.settings.value("geometry")
        if geometry:
            self.restoreGeometry(geometry)
            
        state = self.settings.value("windowState")
        if state:
            self.restoreState(state)
            
    def save_settings(self) -> None:
        self.settings.setValue("geometry", self.saveGeometry())
        self.settings.setValue("windowState", self.saveState())
        
    def closeEvent(self, event) -> None:
        self.save_settings()
        event.accept()


def main() -> None:
    if not HAS_QT:
        print("❌ PyQt6 is required for GUI functionality.")
        print("Install with: pip install policy-guardian[gui]")
        sys.exit(1)
        
    app = QApplication(sys.argv)
    app.setApplicationName("Policy Guardian")
    app.setApplicationVersion(__version__)
    
    try:
        window = PolicyGuardianGUI(app)
        window.show()
        sys.exit(app.exec())
        
    except Exception as e:
        QMessageBox.critical(
            None,
            "Fatal Error",
            f"A fatal error occurred:\n{str(e)}\n\n{traceback.format_exc()}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main() 