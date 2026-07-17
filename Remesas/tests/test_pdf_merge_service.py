from datetime import datetime
from pathlib import Path
import tempfile
import unittest

from pypdf import PdfReader, PdfWriter
from services.pdf_merge_service import (MergeablePdfDocument, PdfMergeCancelled,
    PdfMergeService, PdfValidationStatus)


def document(path, *, identifier=1, member=1):
    return MergeablePdfDocument(identifier,"PDF_MEMBER","batch",1,"Remesa","2026","DIRECTO",member,"Socio",("VE1",),"GENERATED","ACTIVE",Path(path),datetime.now())


def pdf(path, widths):
    writer=PdfWriter()
    for width in widths: writer.add_blank_page(width=width,height=100)
    with Path(path).open("wb") as stream: writer.write(stream)


class PdfMergeServiceTests(unittest.TestCase):
    def setUp(self): self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name); self.service=PdfMergeService()
    def tearDown(self): self.temp.cleanup()

    def test_merge_preserves_order_and_pages(self):
        one=self.root/"uno.pdf"; two=self.root/"dos.pdf"; pdf(one,[101]); pdf(two,[202,203])
        result=self.service.merge_documents([document(one),document(two,identifier=2,member=2)],self.root/"salida.pdf")
        reader=PdfReader(result.output_path); self.assertEqual(result.page_count,3); self.assertEqual([int(p.mediabox.width) for p in reader.pages],[101,202,203])

    def test_missing_corrupt_and_duplicate_are_excluded(self):
        valid=self.root/"válido con espacios.pdf"; corrupt=self.root/"roto.pdf"; pdf(valid,[100]); corrupt.write_text("no pdf")
        docs=[document(valid),document(self.root/"falta.pdf",identifier=2),document(corrupt,identifier=3),document(valid,identifier=4,member=4)]
        statuses=[x.status for x in self.service.validate_documents(docs).items]
        self.assertEqual(statuses,[PdfValidationStatus.VALID,PdfValidationStatus.MISSING,PdfValidationStatus.CORRUPT,PdfValidationStatus.DUPLICATE])

    def test_existing_output_uses_version_suffix(self):
        source=self.root/"a.pdf"; output=self.root/"salida.pdf"; pdf(source,[100]); output.touch()
        self.assertEqual(self.service.merge_documents([document(source)],output).output_path.name,"salida_v2.pdf")

    def test_cancel_removes_partial_output(self):
        source=self.root/"a.pdf"; output=self.root/"salida.pdf"; pdf(source,[100])
        with self.assertRaises(PdfMergeCancelled): self.service.merge_documents([document(source)],output,should_cancel=lambda:True)
        self.assertFalse(output.exists())

    def test_three_hundred_documents(self):
        docs=[]
        for i in range(300):
            path=self.root/f"d{i}.pdf"; pdf(path,[100+i]); docs.append(document(path,identifier=i+1,member=i+1))
        result=self.service.merge_documents(docs,self.root/"grande.pdf")
        self.assertEqual(result.page_count,300)


if __name__ == "__main__": unittest.main()
