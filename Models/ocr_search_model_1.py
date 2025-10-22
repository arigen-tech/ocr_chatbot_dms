import os
import sqlite3
import threading
import subprocess
import pdfplumber
import pytesseract
import sys
import contextlib
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
from docx import Document
from docx.opc.constants import RELATIONSHIP_TYPE as RT
from pyzbar.pyzbar import decode
from PIL import Image
import pandas as pd
import openpyxl
import json

class DocumentHandler(FileSystemEventHandler):
    """Handler for document file system events."""
    def __init__(self, document_processor):
        self.document_processor = document_processor
        self.image_extensions = ['.png', '.jpg', '.jpeg', '.bmp', '.gif', '.tiff', '.webp']
        self.word_extensions = ['.docx', '.doc', '.docm', '.dotx', '.dotm']
        self.excel_extensions = ['.xlsx', '.xls', '.xlsm', '.csv', '.xlsb']
        self.valid_extensions = ['.pdf', '.txt'] + self.image_extensions + self.word_extensions + self.excel_extensions

    def on_created(self, event):
        file_path = event.src_path
        
        if not any(file_path.endswith(ext) for ext in self.valid_extensions):
            return
            
        print(f"\nNew document detected: {file_path}")
        self.document_processor.process_single_document(file_path)

class DocumentProcessor:
    _instance = None
    _lock = threading.Lock()
    
    @classmethod
    # def get_instance(cls, base_dir="/home/ubuntu/dms_project/dms_documents", db_name="search-docs.db"):
    # def get_instance(cls, base_dirs=["PDFs_Data", "Documentation"], db_name="search-docs.db"):
    def get_instance(cls, base_dirs=["E:\FTP\DMS_Document"], db_name="search-docs.db"):
        """Singleton accessor that supports multiple directories."""
        if not cls._instance:
            cls._instance = cls(base_dirs, db_name)
            cls._instance.load_existing_documents()
        return cls._instance

    def __init__(self, base_dirs, db_name):
        """Initialize with multiple base directories."""
        if not base_dirs or not db_name:
            raise ValueError("base_dirs and db_name are required")
        self.base_dirs = [Path(d) for d in base_dirs]
        self.db_path = self.base_dirs[0] / db_name  # Store DB in first folder
        for base_dir in self.base_dirs:
            base_dir.mkdir(parents=True, exist_ok=True)
        self._initialize_db()
    
    def _get_valid_extensions(self):
        """Return a list of supported extensions."""
        return ['.pdf', '.txt', '.png', '.jpg', '.jpeg', '.bmp', '.gif', '.tiff', '.webp',
                '.docx', '.doc', '.docm', '.dotx', '.dotm',
                '.xlsx', '.xls', '.xlsm', '.csv', '.xlsb']

    def _initialize_db(self):
        """Create the db file inside base_dir if it doesn't exist or recreate to include missing columns."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Check if the table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='document_data';")
        table_exists = cursor.fetchone()

        if table_exists:
            # Fetch existing data to migrate
            cursor.execute("SELECT file_name, content FROM document_data;")
            existing_data = cursor.fetchall()

            # Drop the old virtual table
            cursor.execute("DROP TABLE document_data;")
            conn.commit()
        
        # Create the new virtual table with the qr_data column
        cursor.execute("""
            CREATE VIRTUAL TABLE document_data USING FTS5(
                file_name, content, qr_data
            );
        """)
        conn.commit()

        # Restore the old data into the newly created table
        if table_exists:
            cursor.executemany("INSERT INTO document_data (file_name, content) VALUES (?, ?);", existing_data)
            conn.commit()

        conn.close()

    def get_db_connection(self):
        """Create a new database connection for the current thread."""
        return sqlite3.connect(self.db_path)

    def load_existing_documents(self):
        """Scan all base directories and index documents."""
        doc_files = []
        valid_exts = set(self._get_valid_extensions())

        for base_dir in self.base_dirs:
            doc_files.extend(f for f in base_dir.rglob("*") if f.suffix.lower() in valid_exts)

        if not doc_files:
            print("No Documents found across configured directories.")
            return

        print(f"Loading {len(doc_files)} Documents into the database...")
        for doc_path in doc_files:
            self.process_single_document(str(doc_path))

        print("Database initialization complete.")

    def extract_text_from_pdf(self, doc_path):
        """Extract text from PDF files."""
        full_text = ""
        qr_content = set()

        # Helper to silence C-level stderr (zbar assertions) during decode calls
        @contextlib.contextmanager
        def _suppress_stderr():
            try:
                with open(os.devnull, 'w') as devnull:
                    old_stderr_fno = os.dup(2)
                    os.dup2(devnull.fileno(), 2)
                    try:
                        yield
                    finally:
                        os.dup2(old_stderr_fno, 2)
                        os.close(old_stderr_fno)
            except Exception:
                # If dup/dup2 not available or fails, fall back to contextlib.redirect_stderr
                with contextlib.redirect_stderr(sys.stderr):
                    yield

        try:
            with pdfplumber.open(doc_path) as pdf:
                for page in pdf.pages:
                    try:
                        text = page.extract_text(x_tolerance=2)
                        if text:
                            full_text += text + "\n"
                    except Exception as page_text_e:
                        print(f"Warning: could not extract page text for {doc_path}: {page_text_e}")

                    # Render page to an image for OCR and QR scanning. Convert to RGB to be safe.
                    try:
                        img_pil = page.to_image(resolution=300).original.convert('RGB')
                    except Exception as img_e:
                        print(f"Warning: could not render page image for OCR for {doc_path}: {img_e}")
                        continue

                    try:
                        ocr_text = pytesseract.image_to_string(img_pil)
                        if ocr_text:
                            full_text += ocr_text + "\n"
                    except Exception as ocr_e:
                        print(f"Warning: pytesseract failed on page image for {doc_path}: {ocr_e}")

                    # Decode QR codes, but suppress noisy zbar stderr and catch exceptions so one bad page
                    # won't stop the whole document processing.
                    try:
                        with _suppress_stderr():
                            qr_codes = []
                            try:
                                qr_codes = decode(img_pil)
                            except Exception as decode_e:
                                # pyzbar sometimes raises on malformed images; log and continue
                                print(f"Warning: pyzbar.decode failed for {doc_path}: {decode_e}")

                            for qr in qr_codes or []:
                                try:
                                    qr_content.add(qr.data.decode())
                                except Exception:
                                    # If decoding bytes fails, ignore this QR
                                    pass
                    except Exception as stderr_e:
                        # If suppression wrapper fails, continue without breaking whole PDF
                        print(f"Warning: failed suppressing stderr for QR decode on {doc_path}: {stderr_e}")

            # Return text (possibly empty) and qr_data (None if empty)
            return full_text.strip(), "; ".join(sorted(qr_content)) if qr_content else None

        except Exception as e:
            print(f"Error extracting from PDF {doc_path}: {str(e)}")
            return None, None
        
    def convert_to_docx(self, input_path):
        """ Convert .doc, .docm, .dotx, .dotm to .docx using unoconv. Always returns a string (output path) or None."""
        input_path = str(input_path)  # Ensure string for subprocess
        output_path = os.path.splitext(input_path)[0] + ".docx"
        try:
            subprocess.run(["unoconv", "-f", "docx", "-o", output_path, input_path], check=True)
            if os.path.exists(output_path):
                return output_path  # always a string
            else:
                print(f"Conversion failed: {input_path} → {output_path}")
                return None
        except subprocess.CalledProcessError as e:
            print(f"Error converting {input_path} to .docx: {str(e)}")
            return None
    
    def extract_text_from_word(self, doc_path):
        """Extract text from various Word formats, including images."""
        ext = os.path.splitext(doc_path)[1].lower()
        if ext in ['.doc', '.docm', '.dotx', '.dotm']:
            converted_path = self.convert_to_docx(doc_path)
            if not converted_path:
                return None
            doc_path = converted_path

        if not os.path.exists(doc_path):
            print(f"File does not exist: {doc_path}")
            return None

        try:
            doc = Document(doc_path)
            text_data = "\n".join([para.text for para in doc.paragraphs])

            # Process images in the document
            image_texts = self.extract_text_from_word_images(doc)

            return text_data + "\n\n" + image_texts
        except Exception as e:
            print(f"Error extracting text from {doc_path}: {str(e)}")
            return None
    
    def extract_text_from_word_images(self, doc):
        """Extract text from images in the Word document."""
        image_texts = []
        instance = DocumentProcessor.get_instance()
        temp_dir = instance.base_dirs[0] / "temp_images"
        temp_dir.mkdir(parents=True, exist_ok=True)
        for rel in doc.part.rels.values():
            if rel.reltype == RT.IMAGE:
                image_part = rel.target_part
                image_bytes = image_part.blob
                image_ext = os.path.splitext(image_part.partname)[1].lower()
                image_path = temp_dir / f"image{len(image_texts)}{image_ext}"

                with open(image_path, "wb") as f:
                    f.write(image_bytes)

                try:
                    text = pytesseract.image_to_string(Image.open(str(image_path)))
                    image_texts.append(text.strip())
                except Exception as e:
                    print(f"Error extracting text from image {image_path}: {str(e)}")

        return "\n".join(image_texts)
    
    def extract_text_from_txt(self, txt_path):
        """Extract text from .txt files."""
        try:
            with open(txt_path, 'r', encoding='utf-8') as file:
                return file.read()
        except Exception as e:
            print(f"Error extracting from TXT {txt_path}: {str(e)}")
            return None
    
    def extract_text_from_image(self, img_path):
        """Extract text and QR codes from images, supporting all common formats."""
        try:
            img_pil = Image.open(img_path).convert('RGB')
            text = ""
            try:
                text = pytesseract.image_to_string(img_pil)
            except Exception as ocr_e:
                print(f"Warning: pytesseract failed on image {img_path}: {ocr_e}")

            qr_content = None
            try:
                # Suppress zbar assertions and guard decode
                with open(os.devnull, 'w') as devnull:
                    old_stderr_fno = os.dup(2)
                    os.dup2(devnull.fileno(), 2)
                    try:
                        qr_codes = decode(img_pil)
                    finally:
                        os.dup2(old_stderr_fno, 2)
                        os.close(old_stderr_fno)

                if qr_codes:
                    qr_content = "; ".join(qr.data.decode() for qr in qr_codes)
            except Exception as decode_e:
                print(f"Warning: pyzbar.decode failed for image {img_path}: {decode_e}")

            return text.strip() if text else "", qr_content
        except Exception as e:
            print(f"Error extracting from Image {img_path}: {str(e)}")
            return None, None
        
    def extract_text_from_excel(self, excel_path):
        """Extract text from Excel files, supporting all standard formats."""
        try:
            if excel_path.endswith('.csv'):
                # Handle CSV using pandas
                df = pd.read_csv(excel_path)
                return df.to_string()

            elif excel_path.endswith(('.xls', '.xlsx', '.xlsm', '.xlsb')):
                # Handle Excel files using openpyxl for better compatibility
                wb = openpyxl.load_workbook(excel_path, data_only=True)
                sheets_text = []

                for sheet in wb.worksheets:
                    for row in sheet.iter_rows(values_only=True):
                        sheets_text.append(" ".join([str(cell) for cell in row if cell]))

                return "\n".join(sheets_text)

            else:
                return None  # Unsupported format
        except Exception as e:
            print(f"Error extracting from Excel {excel_path}: {str(e)}")
            return None

    def is_file_in_database(self, conn, file_name):
        """Check if a file exists in the database."""
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM document_data WHERE file_name = ?", (file_name,))
        return cursor.fetchone() is not None
    
    def process_queue(self):
        """Process Documents from the queue continuously using parallel processing."""
        while True:
            try:
                doc_path = self.processing_queue.get()
                self.executor.submit(self.process_single_document, doc_path)  # Async processing
                self.processing_queue.task_done()
            except Exception as e:
                print(f"Error processing document from queue: {e}")
    
    def log_failed_file(self, file_name):
        """ Stores failed/corrupted/unreadable file names in failed_files.json as a JSON list. Ensures uniqueness—no duplicate names."""
        failed_log_path = self.db_path.parent / "failed_files.json"
        try:
            if failed_log_path.exists():
                with open(failed_log_path, "r", encoding="utf-8") as f:
                    existing_names = set(json.load(f))
            else:
                existing_names = set()
            
            if file_name not in existing_names:
                existing_names.add(file_name)
                with open(failed_log_path, "w", encoding="utf-8") as f:
                    json.dump(sorted(existing_names), f, ensure_ascii=False, indent=2)
        except Exception as log_e:
            print(f"[Log Error] Could not write to failed_files.json: {log_e}")

    def process_single_document(self, doc_path):
        """Process a document while ensuring it's not duplicated in the database."""
        conn = self.get_db_connection()
        try:
            doc_path = Path(doc_path)
            file_name = doc_path.name

            handler = DocumentHandler(self)

            if doc_path.suffix.lower() not in handler.valid_extensions:
                print(f"Skipping unsupported file type: {file_name}")
                return
            
            if self.is_file_in_database(conn, file_name):
                print(f"Skipping duplicate file: {file_name}")
                return

            print(f"Processing and inserting: {file_name}")

            text, qr_data = None, None
            
            try:
                if doc_path.suffix.lower() in handler.image_extensions:
                    text, qr_data = self.extract_text_from_image(doc_path)
                elif doc_path.suffix.lower() in handler.word_extensions:
                    text = self.extract_text_from_word(doc_path)
                elif doc_path.suffix.lower() in handler.excel_extensions:
                    text = self.extract_text_from_excel(doc_path)
                elif doc_path.suffix.lower() == ".pdf":
                    text, qr_data = self.extract_text_from_pdf(doc_path)
                elif doc_path.suffix.lower() == ".txt":
                    text = self.extract_text_from_txt(doc_path)

            except Exception as inner_e:
                print(f"Exception while extracting from {file_name}: {inner_e}")
                self.log_failed_file(file_name)
                return
            
            text_is_empty = (text is None or (isinstance(text, str) and not text.strip()))
            qr_missing_in_pdf_or_image = (
                (doc_path.suffix.lower() in handler.image_extensions + [".pdf"])
                and qr_data is None
            )

            if text_is_empty:
                print(f"Failed to extract text from: {file_name}")
                self.log_failed_file(file_name)
                return

            
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO document_data (file_name, content, qr_data) VALUES (?, ?, ?);",
                (file_name, text, qr_data)
            )
            conn.commit()
            print(f"Successfully stored: {file_name}")
                
        finally:
            conn.close()

    def get_all_documents(self):
        """Retrieve all Documents names stored in the database."""
        conn = self.get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT file_name FROM document_data")
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

    def search_database(self, query, selected_files=None):
        """Search the database for documents containing the specified query, including special characters."""
        conn = self.get_db_connection()
        try:
            cursor = conn.cursor()

            fts_query = f'"{query}"'

            if selected_files:
                placeholders = ','.join('?' * len(selected_files))
                match_sql = f"""
                    SELECT DISTINCT file_name 
                    FROM document_data 
                    WHERE content MATCH ? 
                    AND file_name IN ({placeholders})
                """
                cursor.execute(match_sql, (fts_query, *selected_files))
            else:
                cursor.execute("SELECT DISTINCT file_name FROM document_data WHERE content MATCH ?", (fts_query,))
            
            results = [row[0] for row in cursor.fetchall()]

            # Fallback to LIKE if MATCH fails or returns nothing
            if not results:
                wildcard_query = f"%{query}%"
                if selected_files:
                    like_sql = f"""
                        SELECT DISTINCT file_name 
                        FROM document_data 
                        WHERE content LIKE ? 
                        AND file_name IN ({placeholders})
                    """
                    cursor.execute(like_sql, (wildcard_query, *selected_files))
                else:
                    cursor.execute("SELECT DISTINCT file_name FROM document_data WHERE content LIKE ?", (wildcard_query,))
                results = [row[0] for row in cursor.fetchall()]

            return results
        finally:
            conn.close()

    
    def clean_database(self):
        """Delete all entries in the document_data table, effectively resetting the database."""
        conn = self.get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM document_data")
            conn.commit()
            print("Database cleaned successfully.")
        except sqlite3.Error as e:
            print(f"Database error while cleaning: {str(e)}")
        finally:
            conn.close()

    def start_processing(self):
        """Begin monitoring all folders with Watchdog."""
        self.load_existing_documents()

        observer = Observer()
        event_handler = DocumentHandler(self)

        for base_dir in self.base_dirs:
            observer.schedule(event_handler, str(base_dir), recursive=True)

        observer.start()
        return observer