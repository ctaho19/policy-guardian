from PyQt6.QtWidgets import (QMainWindow, QVBoxLayout, QHBoxLayout, QWidget, QPushButton, 
                             QTextEdit, QFileDialog, QLabel, QCheckBox, QGroupBox)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QIcon
from PyQt6.QtPrintSupport import QPrinter, QPrintDialog
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
import importlib
import os
import sys
import datetime

class ValidatorThread(QThread):
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, validator_function, xml_file):
        super().__init__()
        self.validator_function = validator_function
        self.xml_file = xml_file

    def run(self):
        try:
            results = self.validator_function(self.xml_file)
            self.finished.emit(results)
        except Exception as e:
            self.error.emit(str(e))

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PolicyGuardian")
        self.setWindowIcon(QIcon('/Users/chris/Downloads/PA Policy Analyzer/PolicyGuardian/logo.png'))
        self.setMinimumSize(800, 600)

        main_layout = QVBoxLayout()

        # File selection
        file_layout = QHBoxLayout()
        self.file_path_label = QLabel("No file selected")
        file_select_button = QPushButton("Select XML File")
        file_select_button.clicked.connect(self.select_file)
        file_layout.addWidget(self.file_path_label)
        file_layout.addWidget(file_select_button)
        main_layout.addLayout(file_layout)

        # Validator selection
        validator_group = QGroupBox("Validators")
        validator_layout = QVBoxLayout()
        validator_group.setLayout(validator_layout)

        self.validator_checkboxes = {}
        validators = ["ShadowingRules", "OverlyPermissiveRules", "RulesMissingSecurityProfile", "ZoneBasedChecks", "RedundantRule"]
        for validator_name in validators:
            checkbox = QCheckBox(validator_name)
            self.validator_checkboxes[validator_name] = checkbox
            validator_layout.addWidget(checkbox)

        # Select All and Clear buttons
        button_layout = QHBoxLayout()
        self.select_all_button = QPushButton("Select All")
        self.select_all_button.clicked.connect(self.select_all_validators)
        self.clear_button = QPushButton("Clear")
        self.clear_button.clicked.connect(self.clear_validators)
        button_layout.addWidget(self.select_all_button)
        button_layout.addWidget(self.clear_button)
        validator_layout.addLayout(button_layout)

        main_layout.addWidget(validator_group)

        # Run button
        self.run_button = QPushButton("Run Validators")
        self.run_button.clicked.connect(self.run_validators)
        main_layout.addWidget(self.run_button)

        # Export button
        self.export_button = QPushButton("Export Report")
        self.export_button.clicked.connect(self.export_report)
        main_layout.addWidget(self.export_button)

        # Results display
        self.results_display = QTextEdit()
        self.results_display.setReadOnly(True)
        main_layout.addWidget(self.results_display)

        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)

        self.xml_file = None
        self.report_generator = None
        self.report_content = None

    def select_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select XML File", "", "XML Files (*.xml)")
        if file_path:
            self.file_path_label.setText(file_path)
            self.xml_file = file_path

    def select_all_validators(self):
        for checkbox in self.validator_checkboxes.values():
            checkbox.setChecked(True)

    def clear_validators(self):
        for checkbox in self.validator_checkboxes.values():
            checkbox.setChecked(False)

    def run_validators(self):
        if not self.xml_file:
            self.results_display.setText("Please select a valid XML file first.")
            return

        script_dir = os.path.dirname(os.path.abspath(__file__))
        test_scripts_dir = os.path.join(script_dir, "Test Scripts")
        sys.path.append(test_scripts_dir)

        self.results_display.clear()
        self.report_generator = ReportGenerator(self.xml_file)
        results = {}

        for validator_name, checkbox in self.validator_checkboxes.items():
            if checkbox.isChecked():
                try:
                    validator_module = importlib.import_module(validator_name)
                    validator_function = getattr(validator_module, f"check_{validator_name.lower()}")
                    
                    validator_results = validator_function(self.xml_file)
                    results[validator_name] = validator_results
                    self.handle_validator_result(validator_name, validator_results)
                except ImportError as e:
                    self.handle_validator_error(validator_name, f"Failed to import module: {str(e)}")
                except AttributeError as e:
                    self.handle_validator_error(validator_name, f"Failed to find validator function: {str(e)}\nExpected function name: check_{validator_name.lower()}")
                except Exception as e:
                    self.handle_validator_error(validator_name, f"An error occurred: {str(e)}")

        self.report_content = self.report_generator.generate_report(results)
        self.results_display.append("\nReport generated. Click 'Export Report' to save as PDF.")

    def handle_validator_result(self, validator_name, result):
        formatted_result = f"Results for {validator_name}:\n"
        if isinstance(result, list):
            for item in result:
                if isinstance(item, tuple):
                    formatted_result += f"- {item[0]}: {item[1]}\n"
                else:
                    formatted_result += f"- {item}\n"
        else:
            formatted_result += str(result)
        formatted_result += "\n"
        self.results_display.append(formatted_result)

    def handle_validator_error(self, validator_name, error):
        error_message = f"Error in {validator_name}: {error}\n"
        self.results_display.append(error_message)

    def export_report(self):
        if not self.report_content:
            self.results_display.append("No report to export. Please run the validators first.")
            return

        file_path, _ = QFileDialog.getSaveFileName(self, "Save Report", "", "PDF Files (*.pdf)")
        if file_path:
            self.report_generator.save_report(self.report_content, file_path)
            self.results_display.append(f"\nReport saved to: {file_path}")

class ReportGenerator:
    def __init__(self, xml_file):
        self.xml_file = xml_file
        self.validators = {
            "ShadowingRules": "Detects rules that may be overshadowed by other rules",
            "OverlyPermissiveRules": "Identifies rules that may be too permissive",
            "RulesMissingSecurityProfile": "Finds rules without proper security profiles",
            "ZoneBasedChecks": "Checks for zone-related issues in rules",
            "RedundantRule": "Identifies potentially redundant rules"
        }

    def generate_report(self, results):
        report = []
        styles = getSampleStyleSheet()
        styles['Heading2'].fontSize = 14
        styles['Heading2'].spaceBefore = 12
        styles['Heading2'].spaceAfter = 6
        styles.add(ParagraphStyle(name='BodyCustom', fontSize=10, spaceBefore=6, spaceAfter=6))

        # Title
        report.append(Paragraph("Firewall Policy Audit Report", styles['Heading1']))
        report.append(Spacer(1, 12))

        # Firewall details
        report.append(Paragraph("Firewall Configuration Details", styles['Heading2']))
        report.append(Paragraph(f"Configuration file: {self.xml_file}", styles['BodyCustom']))
        report.append(Paragraph(f"Audit Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['BodyCustom']))
        report.append(Spacer(1, 12))

        # Validation Results
        report.append(Paragraph("Validation Results", styles['Heading2']))

        for validator, description in self.validators.items():
            report.append(Paragraph(f"{validator}", styles['Heading3']))
            report.append(Paragraph(f"Description: {description}", styles['BodyCustom']))
            
            if validator in results:
                if results[validator]:
                    data = [["Rule Name", "Issue"]]
                    for item in results[validator]:
                        if isinstance(item, tuple):
                            data.append([item[0], item[1]])
                        else:
                            data.append([item, ""])
                    
                    t = Table(data, colWidths=[200, 300])
                    t.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 12),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
                        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                        ('FONTSIZE', (0, 1), (-1, -1), 10),
                        ('TOPPADDING', (0, 1), (-1, -1), 6),
                        ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black)
                    ]))
                    report.append(t)
                    report.append(Spacer(1, 12))

        return report

    def save_report(self, report, file_path):
        doc = SimpleDocTemplate(file_path, pagesize=letter)
        doc.build(report)

if __name__ == "__main__":
    import sys
    from PyQt6.QtWidgets import QApplication
    
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
