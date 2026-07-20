from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import hashlib
import logging
from pathlib import Path
from time import monotonic
from typing import Callable, Iterable

from pypdf import PdfReader, PdfWriter

logger = logging.getLogger(__name__)


class PdfValidationStatus(str, Enum):
    VALID="VALID"; MISSING="MISSING"; CORRUPT="CORRUPT"; ENCRYPTED="ENCRYPTED"; EMPTY="EMPTY"; DUPLICATE="DUPLICATE"


@dataclass(frozen=True)
class MergeablePdfDocument:
    document_id: int | None; document_kind: str; batch_id: str | None
    remittance_id: int | None; remittance_name: str; campaign: str; company: str; crop: str
    member_id: int | None; member_name: str; id_liqs: tuple[str, ...]
    document_status: str; batch_status: str; file_path: Path
    generated_at: datetime | None; page_count: int | None=None; file_size: int | None=None
    selected: bool=False


@dataclass(frozen=True)
class PdfValidationItem:
    document: MergeablePdfDocument; status: PdfValidationStatus; page_count: int=0; detail: str=""


@dataclass(frozen=True)
class PdfValidationResult:
    items: tuple[PdfValidationItem, ...]
    @property
    def valid(self): return tuple(x for x in self.items if x.status is PdfValidationStatus.VALID)
    @property
    def excluded(self): return tuple(x for x in self.items if x.status is not PdfValidationStatus.VALID)


@dataclass(frozen=True)
class PdfMergeResult:
    output_path: Path; documents_included: int; documents_excluded: int; page_count: int; validation: PdfValidationResult


class PdfMergeCancelled(Exception): pass


class PdfMergeService:
    def __init__(self, repository=None): self.repository=repository

    @staticmethod
    def _date(value):
        if not value: return None
        try: return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError: return None

    def list_filter_options(self, *, document_kind, campaign=None, company=None, crop=None):
        options=self.repository.list_document_filter_options(document_kind=document_kind,campaign=campaign,company=company,crop=crop)
        logger.info("[PdfMergeFilterOptions] type=%s campaign=%s company=%s crop=%s campaigns=%s companies=%s crops=%s remittances=%s",document_kind,campaign,company,crop,len(options["campaigns"]),len(options["companies"]),len(options["crops"]),len(options["remittances"]))
        return options

    def list_available_documents(self, *, document_kind, campaign=None, company=None, crop=None, remittance_id=None,
                                 member_id=None, date_from=None, date_to=None, status=None, include_voided=False):
        if document_kind not in ("PDF_MEMBER", "PDF_DRAFT"): raise ValueError("Tipo documental no admitido")
        rows=self.repository.list_mergeable_documents(document_kind=document_kind,campaign=campaign,company=company,crop=crop,
            remittance_id=remittance_id,member_id=member_id,date_from=date_from,date_to=date_to,status=status,include_voided=include_voided)
        docs=[]
        for r in rows:
            path=Path(r["file_path"]); size=path.stat().st_size if path.exists() else None
            docs.append(MergeablePdfDocument(r["id"],document_kind,r["batch_id"],r["remittance_id"],
                r["remesa_name"] if document_kind=="PDF_MEMBER" else r["remittance_name"],r["campaign"],r["company"],r["crop"],
                r["recipient_member_id"],r["member_name"],tuple(filter(None,str(r["id_liqs"]).split(" · "))),
                r["status"],r["batch_status"],path,self._date(r["generated_at"]),None,size,False))
        logger.info("[PdfMergeSearch] type=%s campaign=%s company=%s crop=%s remittance_id=%s member_id=%s date_from=%s date_to=%s results=%s",document_kind,campaign,company,crop,remittance_id,member_id,date_from,date_to,len(docs))
        return tuple(docs)

    def validate_documents(self, documents: Iterable[MergeablePdfDocument], progress_callback=None, should_cancel=None):
        items=[]; seen_ids=set(); seen_paths=set(); seen_keys=set(); seen_hashes=set(); docs=tuple(documents)
        for index,doc in enumerate(docs,1):
            if should_cancel and should_cancel(): raise PdfMergeCancelled()
            if progress_callback: progress_callback("validando",index,len(docs),0)
            path=doc.file_path; normalized=str(path.resolve()).casefold(); key=(doc.batch_id,doc.member_id,doc.document_kind)
            if not path.exists() or not path.is_file(): items.append(PdfValidationItem(doc,PdfValidationStatus.MISSING)); continue
            if path.suffix.casefold() != ".pdf" or path.stat().st_size == 0: items.append(PdfValidationItem(doc,PdfValidationStatus.EMPTY)); continue
            try:
                hasher=hashlib.sha256()
                with path.open("rb") as source:
                    for chunk in iter(lambda: source.read(1024*1024), b""): hasher.update(chunk)
                digest=hasher.digest()
                if digest in seen_hashes: items.append(PdfValidationItem(doc,PdfValidationStatus.DUPLICATE)); continue
                with path.open("rb") as stream:
                    reader=PdfReader(stream)
                    if reader.is_encrypted:
                        try:
                            if not reader.decrypt(""): items.append(PdfValidationItem(doc,PdfValidationStatus.ENCRYPTED)); continue
                        except Exception: items.append(PdfValidationItem(doc,PdfValidationStatus.ENCRYPTED)); continue
                    pages=len(reader.pages)
                    if not pages: items.append(PdfValidationItem(doc,PdfValidationStatus.EMPTY)); continue
                duplicate=(doc.document_id is not None and doc.document_id in seen_ids) or normalized in seen_paths or (doc.batch_id is not None and key in seen_keys)
                if duplicate: items.append(PdfValidationItem(doc,PdfValidationStatus.DUPLICATE)); continue
                items.append(PdfValidationItem(doc,PdfValidationStatus.VALID,pages))
                seen_ids.add(doc.document_id); seen_paths.add(normalized); seen_keys.add(key); seen_hashes.add(digest)
            except Exception as exc: items.append(PdfValidationItem(doc,PdfValidationStatus.CORRUPT,detail=str(exc)))
        result=PdfValidationResult(tuple(items)); counts={s.value:sum(x.status is s for x in items) for s in PdfValidationStatus}
        logger.info("[PdfMergeValidation] %s",counts); return result

    @staticmethod
    def available_output_path(path):
        path=Path(path)
        if not path.exists(): return path
        n=2
        while True:
            candidate=path.with_name(f"{path.stem}_v{n}{path.suffix}")
            if not candidate.exists(): return candidate
            n+=1

    def merge_documents(self, documents, output_path, *, overwrite=False, progress_callback=None, should_cancel=None):
        started=monotonic(); output=Path(output_path)
        if output.suffix.casefold() != ".pdf": output=output.with_suffix(".pdf")
        if not overwrite: output=self.available_output_path(output)
        validation=self.validate_documents(documents,progress_callback,should_cancel); writer=PdfWriter(); pages=0
        output.parent.mkdir(parents=True,exist_ok=True)
        logger.info("[PdfMergeStarted] documents=%s output=%s",len(validation.valid),output)
        try:
            for index,item in enumerate(validation.valid,1):
                if should_cancel and should_cancel(): raise PdfMergeCancelled()
                with item.document.file_path.open("rb") as stream:
                    reader=PdfReader(stream)
                    for page in reader.pages: writer.add_page(page); pages+=1
                if progress_callback: progress_callback("uniendo",index,len(validation.valid),pages)
            if not pages: raise ValueError("No hay documentos PDF válidos para unificar.")
            with output.open("wb") as stream: writer.write(stream)
        except PdfMergeCancelled:
            output.unlink(missing_ok=True); logger.info("[PdfMergeCancelled] output=%s",output); raise
        except Exception: output.unlink(missing_ok=True); logger.exception("[PdfMergeFailed] output=%s",output); raise
        logger.info("[PdfMergeCompleted] documents=%s pages=%s output=%s duration=%.3f",len(validation.valid),pages,output,monotonic()-started)
        return PdfMergeResult(output,len(validation.valid),len(validation.excluded),pages,validation)
