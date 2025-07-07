import gc
import io
import re
from typing import Set, List, Union, Tuple

import boto3
import pdfplumber
from memory_profiler import profile
from pdf2image import convert_from_bytes
from pdfplumber.table import Table
from pymongo import MongoClient
from pymongo.server_api import ServerApi

from preprocessing.kkk import ArticleTemp, StatusOfText, TextSplit, ArticleSplitter, SectionSplitter, \
    get_mongodb_collection, MultiLineTextExtractor, LegalActPage
from preprocessing.pdf_boundaries.margin_boundaries import LegalActMarginArea
from preprocessing.pdf_boundaries.paragraph_boundaries import LegalActParagraphArea, ParagraphAreaType
from preprocessing.pdf_utils.table_utils import TableDetector, TableCoordinates
from preprocessing.utils.attachments_extraction import \
    AttachmentRegion, PageRegions
from preprocessing.utils.defaults import AWS_REGION
from preprocessing.utils.page_regions import LegalActPageRegionParagraphs, LegalActPageRegionMarginNotes, \
    LegalActPageRegionFooterNotes, LegalActPageRegionAttachment, LegalActPageRegion
from preprocessing.utils.s3_helper import extract_bucket_and_key
from preprocessing.utils.stages_objects import EstablishLegalWithTablesResult


def get_pages_with_attachments(pdf: pdfplumber.pdf.PDF) -> Set[int]:
    """
    Find pages with attachments in a PDF
    :param pdf: pdfplumber.pdf.PDF object
    :return: List of page numbers with attachments
    """
    attachment_pages = set()
    for page_index in range(len(pdf.pages)):
        try:
            page = pdf.pages[page_index]
            text = page.extract_text()
            matches = re.finditer(LegalActPageRegion.LEGAL_ANNOTATION_PATTERN_1, text, re.DOTALL)
            if any(matches):
                for page_from_index in range(page_index, len(pdf.pages)):
                    page_iter = pdf.pages[page_from_index]
                    attachment_pages.add(page_iter.page_number)
                    del page_iter
                break
            del text
        finally:
            pdf.flush_cache()
            del page

    return attachment_pages


def get_attachments_headers(pdf: pdfplumber.pdf.PDF, attachments_page_regions: List[Union[LegalActPageRegion, None]]) -> \
        List[str]:
    """
    Find headers of attachments in a PDF
    :param pdf: pdfplumber.pdf.PDF object
    :return: List of attachment headers
    """
    attachment_headers = []
    for attachment_page_region in attachments_page_regions:
        page = pdf.pages[attachment_page_region.page_number - 1]
        page_attachment_bbox = (attachment_page_region.start_x, attachment_page_region.start_y,
                                attachment_page_region.end_x,
                                attachment_page_region.end_y)
        text_of_page_within_attachment = page.within_bbox(page_attachment_bbox).extract_text()
        matches = list(
            re.finditer(LegalActPageRegion.LEGAL_ANNOTATION_PATTERN_1, text_of_page_within_attachment, re.DOTALL))
        for match in matches:
            header = match.group(1).strip()
            if header in attachment_headers:
                raise ValueError(f"Invalid state in finding attachments headers: {header} already found.")
            attachment_headers.append(header)
        del page
    return attachment_headers


@profile
def declare_part_of_legal_act(document_name: str, pdf_bytes: io.BytesIO):
    attachments_page_regions = []
    page_regions_paragraphs = []
    page_regions_margin_notes = []
    page_regions_footer_notes = []
    attachments_regions = []

    with pdfplumber.open(io.BytesIO(pdf_bytes.getvalue())) as pdf:
        try:
            pages_with_attachments = get_pages_with_attachments(pdf)

            for index in range(len(pdf.pages)):
                page_image = convert_from_bytes(pdf_bytes.getvalue(), first_page=index + 1, last_page=index + 1)[0]
                pdf_page = pdf.pages[index]

                if pdf_page.page_number not in pages_with_attachments:
                    # Build paragraphs and profile memory
                    page_region_text = LegalActPageRegionParagraphs.build(document_name, page_image, pdf_page)
                    page_regions_paragraphs.append(page_region_text)
                    # bbox = (
                    #     page_region_text.start_x, page_region_text.start_y, page_region_text.end_x,
                    #     page_region_text.end_y)
                    # extract_text = pdf_page.within_bbox(bbox).extract_text()

                    margin_notes = LegalActPageRegionMarginNotes.build(document_name, page_image, pdf_page)
                    page_regions_margin_notes.append(margin_notes)
                    # bbox = (margin_notes.start_x, margin_notes.start_y, margin_notes.end_x, margin_notes.end_y)
                    # extract_text_margin = pdf_page.within_bbox(bbox).extract_text()

                    footer_notes = LegalActPageRegionFooterNotes.build(document_name, page_image, pdf_page)
                    if footer_notes:
                        page_regions_footer_notes.append(footer_notes)
                        # bbox = (footer_notes.start_x, footer_notes.start_y, footer_notes.end_x, footer_notes.end_y)
                        # extract_text_footer = pdf_page.within_bbox(bbox).extract_text()
                        # s=4

                else:
                    # Attachments page regions
                    attachments_page_regions.append(LegalActPageRegionAttachment.build(page_image, pdf_page))
                    # Clearing cache of pages to reduce memory usage
                pdf_page.flush_cache()
                pdf_page.get_textmap.cache_clear()
                del page_image, pdf_page
            # Process attachment headers
            attachment_headers = get_attachments_headers(pdf, attachments_page_regions)
            for attachment_header_index in range(len(attachment_headers)):
                attachment_region = AttachmentRegion.build(attachment_header_index, pdf, attachment_headers,
                                                           attachments_page_regions)
                attachments_regions.append(attachment_region)
            # for attachment_region in attachments_regions:
            #     for attachment_page_region in  attachment_region.page_regions:
            #         bbox = (
            #             attachment_page_region.start_x, attachment_page_region.start_y, attachment_page_region.end_x,
            #             attachment_page_region.end_y)
            #         extract_text = pdf.pages[attachment_page_region.page_number-1].within_bbox(bbox).extract_text()
            #         s = 4
            for page_num in pages_with_attachments:
                pdf_page = pdf.pages[page_num - 1]
                # Clearing cache for attachments
                pdf_page.flush_cache()
                pdf_page.get_textmap.cache_clear()
                del pdf_page
        finally:
            del pages_with_attachments
            pdf.close()

    return page_regions_paragraphs, page_regions_margin_notes, page_regions_footer_notes, attachments_regions


article_splitter = ArticleSplitter()
passage_splitter = SectionSplitter()


def get_pages_info(page_regions: PageRegions):
    legal_act_page_info_tab = []
    legal_text_concatenated = io.StringIO()
    current_start_index = -1
    current_end_index = -1
    with pdfplumber.open(pdf_content) as pdf:
        try:
            for paragraph in page_regions.paragraphs:
                page = pdf.pages[paragraph.page_number - 1]
                paragraph_text = page.within_bbox(paragraph.bbox).extract_text().strip()
                text_of_page = paragraph_text + " "
                # legal_text_concatenated += text_of_page
                current_start_index = legal_text_concatenated.tell()
                legal_text_concatenated.write(text_of_page)
                current_end_index = current_start_index + len(paragraph_text)

                # rrr_text = legal_text_concatenated[current_start_index:current_end_index + 1]
                # # w= rrr_text[1142:1145]
                # W = rrr_text[-1]
                legal_act_page_info_tab.append(
                    LegalActPageInfo(page=paragraph, start_index=current_start_index, end_index=current_end_index))
                page.flush_cache()
                page.get_textmap.cache_clear()

                del page
            legal_text_concatenated_value = legal_text_concatenated.getvalue()
            legal_text_concatenated.close()  # Close the StringIO object
        finally:
            pdf.close()
    return legal_act_page_info_tab, legal_text_concatenated_value


@profile
def split_legal_act(row_with_details: EstablishLegalWithTablesResult, pdf_content: io.BytesIO):
    legal_act_page_info_tab, legal_text_concatenated = get_pages_info(
        row_with_details.page_regions)

    # rr = legal_text_concatenated[legal_act_page_info_tab[0].start_index:legal_act_page_info_tab[0].end_index]
    article_splits = article_splitter.split(
        TextSplit(
            text=legal_text_concatenated,
            start_index=0,
            end_index=len(legal_text_concatenated),
            action_type=StatusOfText.NOT_MATCH_CHANGE

        )
    )
    for split in article_splits:
        art_number = article_splitter.get_identificator_text(split)
        cleaned_art_text_split = article_splitter.erase_identificator_from_text(split)
        split_passage(row_with_details, art_number, cleaned_art_text_split, legal_text_concatenated)


def split_passage(row_with_details: EstablishLegalWithTablesResult, art_number, art_split: TextSplit,
                  legal_text_all: str):
    passage_splits = passage_splitter.split(art_split)
    process_passages(row_with_details, art_number, art_split, passage_splits, legal_text_all)
    rr = 4


def process_passages(row_with_details: EstablishLegalWithTablesResult, art_number: str, art_split: TextSplit,
                     passage_splits: List[TextSplit], legal_text_all: str):
    article = ArticleTemp(
        art_number=art_number,
        art_text=art_split,
        passages=[],
        subpoints=[]
    )
    # uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0-pe-1.vxbabj8.mongodb.net/"
    uri = "mongodb+srv://superUser:awglm12345@serverlessinstance0.vxbabj8.mongodb.net/?retryWrites=true&w=majority&appName=ServerlessInstance0"
    db_name = "preprocessing_legal_acts_texts"

    mongo_client = MongoClient(uri, server_api=ServerApi('1'))
    mongo_db = mongo_client[db_name]
    collection = mongo_db["not_correct_spliting_of_passage"]
    # cls.passage_splitter.is_ascending
    passage_numbers = []
    passage_texts = []
    for index_split, passage_split in enumerate(passage_splits):
        is_passage = passage_splitter.can_erase_number(passage_split)
        if index_split == 0 and not is_passage:
            art_text, subpoints = split_subpoint(
                passage_number=None,
                passage_for_further_split=passage_split
            )
        elif is_passage:
            passage_number = passage_splitter.get_identificator_text(passage_split)
            passage_numbers.append(passage_number)
            cleaned_passage_text_split = passage_splitter.erase_identificator_from_text(passage_split)
            if cleaned_passage_text_split.text.strip() == '':
                collection.insert_one({
                    "ELI": row_with_details.general_info.ELI,
                    "art_number": art_number,
                    "art_text": art_split.text,
                    "passage_split": passage_split.text,
                    "type": "passage is empty"
                })
            passage_texts.append(cleaned_passage_text_split.text)
            rrr = legal_text_all[passage_split.start_index:passage_split.end_index]
            miau = legal_text_all[cleaned_passage_text_split.start_index:cleaned_passage_text_split.end_index]
            if not cleaned_passage_text_split.is_up_to_date():
                rau = 4
        else:
            collection.insert_one({
                "ELI": row_with_details.general_info.ELI,
                "art_number": art_number,
                "art_text": art_split.text,
                "passage_split": passage_split.text,
                "type": "passage is not a passage"
            })
    if not passage_splitter.is_ascending(passage_numbers):
        collection.insert_one({
            "ELI": row_with_details.general_info.ELI,
            "art_number": art_number,
            "art_text": art_split.text,
            "passage_splits": [split.text for split in passage_splits],
            "currect_order": passage_numbers,
            "type": "not corrected splitting of passage"
        })


def split_subpoint(passage_number, passage_for_further_split: TextSplit):
    return None, None


pattern = r'(Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+(?:n\s*r\s+\d+[a-z]*|d\s*o\s+u\s*s\s*t\s*a\s*w\s*y)\s*?\)?)(.*?)(?=(?:\s*Z\s*a\s*[łl]\s*[aą]\s*c\s*z\s*n\s*i\s*k\s+(?:n\s*r\s+\d+[a-z]*|d\s*o\s+u\s*s\s*t\s*a\s*w\s*y))|$)'

text = '''
Załącznik nr 4 (uchylony)
Załącznik nr 5 (uchylony)
Załącznik nr 643)
WYKAZ TOWARÓW I USŁUG, OPODATKOWANYCH STAWKĄ PODATKU W WYSOKOŚCI 3 %
                                                                 Poz. Symbol PKWiU Nazwa towaru lub usługi (grupy towarów lub usług)
1 ex 01.11 Zboża, ziemniaki, rośliny przemysłowe i produkty roślinne rolnictwa
pozostałe – z wyłączeniem:
1) arachidów (orzeszków ziemnych) (PKWiU 01.11.32),
2) bawełny i odziarnionych produktów roślinnych dla przemysłu
włókienniczego (PKWiU ex 01.11.7),
3) kauczuku naturalnego (PKWiU 01.11.80),
4) ziół suszonych sortowanych całych
2 01.12 Warzywa, specjalne rośliny ogrodnicze; produkty szkółkarskie
3 01.13.1 Winogrona
4 ex 01.13.23 Owoce pozostałe; ziarna chleba świętojańskiego – z wyłączeniem
ziarna chleba świętojańskiego i chleba świętojańskiego (PKWiU
01.13.23-00.30), papai (PKWiU 01.13.23-00.50), owoców kiwi
(PKWiU 01.13.23-00.60) i owoców południowych pozostałych
osobno niewymienionych (PKWiU ex 01.13.23-00.90)
5 01.13.24-00.10 Orzechy laskowe
6 01.13.24-00.20 Orzechy włoskie
7 ex 01.13.40-00.90 Rośliny przyprawowe pozostałe – wyłącznie surowce roślin
zielarskich oraz nasiona roślin przyprawowych
8 ex 01.2 Zwierzęta żywe i produkty pochodzenia zwierzęcego – z wyłączeniem:
1) wełny (sierści) pranej i włosia surowego pogarbarskiego oraz
preparowanego (PKWiU 01.22.31-00.20, 01.22.32-00.12, -00.22, -
-00.32, -00.42, ex-00.51, -00.52),
2) sierści zwierzęcej cienkiej lub grubej, niezgrzeblonej i nieczesanej
pozostałej (PKWiU 01.22.32-00.90),
3) zwierząt żywych pozostałych (PKWiU 01.25.10), z zastrzeżeniem
pozycji 9,
4) skór surowych świńskich półgarbowanych oraz poubojowych
i zakonserwowanych dla przemysłu garbarskiego (PKWiU
ex 01.25.33-00.00),
5) spermacetu (PKWiU ex 01.25.25)
9 ex 01.25.10 Króliki, pszczoły i jedwabniki
10 ex 02 Produkty gospodarki leśnej – z wyłączeniem drewna surowego
nieobrobionego (PKWiU 02.01.1), bambusa (PKWiU
02.01.42-00.11), faszyny wiklinowej (PKWiU 02.01.42-00.18)
i materiałów roślinnych do produkcji mioteł lub szczotek (PKWiU
02.01.42-00.30)
11 05 Ryby i inne produkty rybołówstwa i rybactwa, z wyłączeniem
produktów połowów pozostałych (PKWiU 05.00.3) i pereł (PKWiU
05.00.4)
12 14.12.20-10.23 Kreda mielona, nawozowa
3) Zgodnie z art. 146 ust. 1 pkt 1 od dnia przystąpienia Rzeczypospolitej Polskiej do Unii Europejskiej do dnia 30 kwietnia
2008 r. stosowało się stawkę podatku od towarów i usług w wysokości 3 % w odniesieniu do czynności, o których mowa
w art. 5 ustawy, których przedmiotem są towary i usługi wymienione w załączniku nr 6 z wyłączeniem
wewnątrzwspólnotowej dostawy towarów i eksportu towarów.'''

text_333 = '''
Dziennik Ustaw – 5 – Poz. 2198
Załączniki do ustawy z dnia 15 grudnia 2016 r.
Załącznik nr 1 do ustawy z dnia 15 grudnia 2016 r. (Dz. U. z 2018 r. poz. 1828)3)
PODZIAŁ KWOT PRZEZNACZONYCH NA REALIZACJĘ PROGRAMU
W FORMACJACH
w tys. zł
Lp. Wyszczególnienie 2017 r. 2018 r. 2019 r. 2020 r. Razem
1 Policja 875 196 1 237 983 2 039 707 1 848 472 6 001 358
2 Straż Graniczna 184 489 278 217 433 458 377 114 1 273 278
3 Państwowa Straż Pożarna 300 407 361 050 542 702 523 319 1 727 478
4 Służba Ochrony Państwa 29 519 45 638 70 819 60 850 206 826
Razem 1 389 611 1 922 888 3 086 686 2 809 755 9 208 940
3) Załącznik nr 1 w brzmieniu ustalonym w załączniku nr 1 do ustawy, o której mowa w odnośniku 1, na podstawie art. 1 pkt 3 tej
ustawy.

14.12.2019
15.11.2019



44.12.2019


'''

import re

# Define the pattern for the last date-like string
pattern = r'\d{2,4}\s*[-\.]\s*\d{2}\s*[-\.]\s*\d{2,4}(?=\s*$)'

match = re.search(pattern, text_333.strip())
w = match.group(0)

FOOTER_PATTERN_NOT_MUST_BE_LAST = r"\d{2,4}\s*[-\.]\s*\d{2}\s*[-\.]\s*\d{2,4}"
s = 4


# Function to extract the last date
def get_last_date(page_text: str) -> str:
    matches = list(re.finditer(FOOTER_PATTERN_NOT_MUST_BE_LAST, page_text))
    # Strip trailing whitespace and match the last date
    if matches:
        last_match = matches[-1]  # Access the last match

        return last_match.group()
    else:
        print("No matches found")
        return None


# Test cases
test_cases = [
    "Some text 14.11.2024",  # Match: "14.11.2024"
    "Some text 13.12.2024  14.11.2024",  # Match: "14.11.2024"
    "Some text with no date here",  # No match
    "13.12.2024",  # Match: "13.12.2024"
    '''
    Załącznik nr 2 do ustawy z dnia 15 grudnia 2016 r. (Dz. U. z 2018 r. poz. 1828)4)
PODZIAŁ KWOT PRZEZNACZONYCH NA REALIZACJĘ
PRZEDSIĘWZIĘĆ PROGRAMU
w tys. zł
Lp. Wyszczególnienie 2017 r. 2018 r. 2019 r. 2020 r. Razem
1 Inwestycje budowlane 332 259 543 134 886 469 584 276 2 346 138
2 Sprzęt transportowy 167 806 282 016 327 816 321 905 1 099 543
Sprzęt uzbrojenia i techniki
3 30 280 37 770 69 984 76 571 214 605
specjalnej
4 Sprzęt informatyki i łączności 167 092 289 169 291 722 283 193 1 031 176
Wyposażenie osobiste
5 82 756 114 017 100 788 75 401 372 962
i ochronne funkcjonariuszy
Wzmocnienie motywacyjnego
6 systemu uposażeń 500 094 538 166 1 160 103 1 207 453 3 405 816
funkcjonariuszy
Zwiększenie konkurencyjności
7 wynagrodzeń pracowników 109 324 118 616 249 804 260 956 738 700
cywilnych
15.11.2019

Razem 1 389 611 1 922 888 3 086 686 2 809 755 9 208 940
4) Załącznik nr 2 w brzmieniu ustalonym w załączniku nr 2 do ustawy, o której mowa w odnośniku 1, na podstawie art. 1 pkt 3 tej
ustawy.


2024-08-09
    sssss
    '''

]

import re

text = """1. First section text.
2. Second section text with more details.
\n3. Third section text.
\n4. Fourth section starts here."""

# Updated regex pattern
pattern = r'(?<=\n)(?=[a-zA-Z[<]\d{1,}[a-z]*\.\s[\D(])'

# Using re.split to divide the text
segments = re.split(pattern, text)

FOOTER_PATTERN_NOT_MUST_BE_LAST = r"\d{2,4}\s*[-\.]\s*\d{2}\s*[-\.]\s*\d{2,4}"

for i, test_case in enumerate(test_cases, 1):
    print(f"Test case {i}: {get_last_date(test_case)}")


def get_footer_region_of_page(footer_notes: List[LegalActPageRegionFooterNotes], page: pdfplumber.pdf.Page) \
        -> LegalActPageRegionFooterNotes:
    footer_note_match = [element for element in footer_notes if element.page_number == page.page_number]
    return footer_note_match[0] if footer_note_match else None


def get_margin_notes_region_of_page(margin_notes: List[LegalActPageRegionMarginNotes], page: pdfplumber.pdf.Page) \
        -> LegalActPageRegionMarginNotes:
    return [element for element in margin_notes if element.page_number == page.page_number][0]


def _is_there_area(page: pdfplumber.pdf.Page, area_bbox: tuple) -> bool:
    area = page.within_bbox(area_bbox)
    area_text = area.extract_text().strip()
    return bool(area_text)


def split_paragraph_into_areas(paragraph: LegalActPageRegionParagraphs, page: pdfplumber.pdf.Page) -> List[
    'LegalActParagraphArea']:
    current_start_y = paragraph.start_y
    current_end_y = paragraph.end_y

    tables_on_page = TableDetector.extract(page, paragraph)

    areas: List['LegalActParagraphArea'] = []
    for table in tables_on_page:

        table_start_y = table.bbox[1]
        table_end_y = table.bbox[3]
        prev_bbox = (paragraph.start_x, current_start_y, paragraph.end_x, table_start_y)
        if current_start_y <= table_start_y and _is_there_area(page, prev_bbox):
            areas.append(LegalActParagraphArea(
                start_x=paragraph.start_x,
                end_x=paragraph.end_x,
                start_y=paragraph.start_y,
                end_y=table_start_y,
                page_number=paragraph.page_number,
                area_type=ParagraphAreaType.REGULAR)
            )
        if _is_there_area(page, table.bbox):
            areas.append(LegalActParagraphArea(
                start_x=table.bbox[0],
                end_x=table.bbox[2],
                start_y=table.bbox[1],
                end_y=table.bbox[3],
                page_number=paragraph.page_number,
                area_type=ParagraphAreaType.TABLE)
            )
        else:
            raise ValueError(
                f"Invalid state: table not found in page text, table bbox: {table.bbox}, page_number: {paragraph.page_number}")
        current_start_y = table_end_y

    last_bbox = (paragraph.start_x, current_start_y, paragraph.end_x, paragraph.end_y)
    if current_start_y < current_end_y and _is_there_area(page, last_bbox):
        areas.append(LegalActParagraphArea(
            start_x=paragraph.start_x,
            end_x=paragraph.end_x,
            start_y=current_start_y,
            end_y=paragraph.end_y,
            page_number=paragraph.page_number,
            area_type=ParagraphAreaType.REGULAR)
        )
    return areas


def _get_sorted_tables(tables_in_page: List[Table], paragraph: LegalActPageRegionParagraphs,
                       page: pdfplumber.pdf.Page) -> List[Table]:
    sorted_tables = []

    bbox = (paragraph.start_x, paragraph.start_y, paragraph.end_x, paragraph.end_y)
    page_paragraph = page.within_bbox(bbox)
    page_text = page_paragraph.extract_text().strip()
    for table_index, table in enumerate(tables_in_page):
        bbox_table_corrected = (
            paragraph.start_x,
            table.bbox[1],
            paragraph.end_x,
            table.bbox[3]
        )
        text_of_table = page_paragraph.within_bbox(bbox_table_corrected).extract_text().strip()

        if text_of_table not in page_text:
            raise ValueError(f"Invalid state: text of table not found in page text, text_of_table: "
                             f"{text_of_table}, page_text: {page_text}, page_number: {page_paragraph.page_number}")

        # Find the start index of the table in the page text
        table_start_index = page_text.index(text_of_table)
        sorted_tables.append((table_start_index, table))

    # Sort tables by their start index
    sorted_tables.sort(key=lambda x: x[0])

    # Return only the sorted tables
    return [table for _, table in sorted_tables]


def _have_overlapping_region( evaluated_table: Table,
                             evaluated_table_page_regions: List[TableCoordinates]
                             ) \
        -> bool:

    table_bbox_corrected = (evaluated_table.page.bbox[0],
                            evaluated_table.bbox[1],
                            evaluated_table.page.bbox[2],
                            evaluated_table.bbox[3])
    for i, page_region in enumerate(evaluated_table_page_regions):
        if _is_bbox_region_is_overlapping(
                table_bbox_corrected,
                page_region.bbox
        ):
            return True

    return False

def _get_overlapping_region( evaluated_table: Table,
                            evaluated_table_page_regions: List[TableCoordinates]) \
        -> Union[Tuple[TableCoordinates, int], None]:
    table_bbox_corrected = (evaluated_table.page.bbox[0],
                            evaluated_table.bbox[1],
                            evaluated_table.page.bbox[2],
                            evaluated_table.bbox[3])
    for i, page_region in enumerate(evaluated_table_page_regions):
        if _is_bbox_region_is_overlapping(
                table_bbox_corrected,
                page_region.bbox
        ):
            return page_region, i

    return None



def _is_bbox_region_is_overlapping(bbox_to_add, bbox_added):
    """Check if two bounding boxes overlap."""
    x1_min, y1_min, x1_max, y1_max = bbox_to_add
    x2_min, y2_min, x2_max, y2_max = bbox_added

    # Check for overlap
    return not (x1_max <= x2_min or x2_max <= x1_min or y1_max <= y2_min or y2_max <= y1_min)

def _is_index_overlapping(index_range1, index_range2):
    """Check if two index ranges overlap."""
    start1, end1 = index_range1
    start2, end2 = index_range2

    # Check for overlap
    return not (end1 <= start2 or end2 <= start1)


def _merge_overlapping_tables( tables_on_page: List[Table]) \
        -> List[TableCoordinates]:
    merged_tables: List[TableCoordinates] = []
    for table in tables_on_page:
        if not _have_overlapping_region(table, merged_tables):
            merged_tables.append(TableCoordinates(
                start_x=table.bbox[0],
                end_x=table.bbox[2],
                start_y=table.bbox[1],
                end_y=table.bbox[3]
            )
            )
        else:
            overlapping_table, merged_index = _get_overlapping_region(table, merged_tables)
            merged_tables[merged_index] = TableCoordinates(
                start_x=min(table.bbox[0], overlapping_table.start_x),
                end_x=max(table.bbox[2], overlapping_table.end_x),
                start_y=min(table.bbox[1], overlapping_table.start_y),
                end_y=max(table.bbox[3], overlapping_table.end_y)
            )
        del table

    return merged_tables

def extract(page: pdfplumber.pdf.Page, paragraph: LegalActPageRegionParagraphs) -> List[TableCoordinates]:
    try:
        tables_in_page = page.find_tables()
        tables_sorted_page = _get_sorted_tables(tables_in_page, paragraph, page)
        return _merge_overlapping_tables(tables_sorted_page)
    finally:
        page.flush_cache()
        page.get_textmap.cache_clear()
        del tables_in_page, tables_sorted_page

@profile
def create_pages(page_regions: PageRegions, pdf_content: io.BytesIO):
    multiline_text_extractor = MultiLineTextExtractor()
    index_of_document = 0
    index_of_footer_document = 0
    index_of_margin_document = 0
    index_of_table = 1
    pages = []
    with pdfplumber.open(pdf_content) as pdf:
        try:
            for paragraph in page_regions.paragraphs:
                page = pdf.pages[paragraph.page_number - 1]
                current_start_y = paragraph.start_y
                current_end_y = paragraph.end_y

                tables_in_page = page.find_tables()
                tables_sorted_page = _get_sorted_tables(tables_in_page, paragraph, page)
                tables_on_page = _merge_overlapping_tables(tables_sorted_page)
                del tables_in_page, tables_sorted_page
                # tables_on_page = extract(page, paragraph) ## TODO this cause memory leaks

                areas: List['LegalActParagraphArea'] = []
                for table in tables_on_page:

                    table_start_y = table.bbox[1]
                    table_end_y = table.bbox[3]
                    prev_bbox = (paragraph.start_x, current_start_y, paragraph.end_x, table_start_y)
                    if current_start_y <= table_start_y and _is_there_area(page, prev_bbox):
                        areas.append(LegalActParagraphArea(
                            start_x=paragraph.start_x,
                            end_x=paragraph.end_x,
                            start_y=paragraph.start_y,
                            end_y=table_start_y,
                            page_number=paragraph.page_number,
                            area_type=ParagraphAreaType.REGULAR)
                        )
                    if _is_there_area(page, table.bbox):
                        areas.append(LegalActParagraphArea(
                            start_x=table.bbox[0],
                            end_x=table.bbox[2],
                            start_y=table.bbox[1],
                            end_y=table.bbox[3],
                            page_number=paragraph.page_number,
                            area_type=ParagraphAreaType.TABLE)
                        )
                    else:
                        raise ValueError(
                            f"Invalid state: table not found in page text, table bbox: {table.bbox}, page_number: {paragraph.page_number}")
                    current_start_y = table_end_y

                last_bbox = (paragraph.start_x, current_start_y, paragraph.end_x, paragraph.end_y)
                if current_start_y < current_end_y and _is_there_area(page, last_bbox):
                    areas.append(LegalActParagraphArea(
                        start_x=paragraph.start_x,
                        end_x=paragraph.end_x,
                        start_y=current_start_y,
                        end_y=paragraph.end_y,
                        page_number=paragraph.page_number,
                        area_type=ParagraphAreaType.REGULAR)
                    )
                paragraph_areas = areas

                margin = get_margin_notes_region_of_page(page_regions.margin_notes, page)
                margin_areas = LegalActMarginArea.split_margin_notes_into_areas(paragraph, margin, page)

                w = 4
                if paragraph.page_number == 9 or paragraph.page_number == 4:
                    s = 4

                ## TODO uncomment this
                legal_act_page = LegalActPage.build(
                    page=page,
                    paragraph_areas=paragraph_areas,
                    margin_areas=margin_areas,
                    footer_area=get_footer_region_of_page(page_regions.footer_notes, page),
                    current_index_of_legal_act=index_of_document,
                    current_table_number=index_of_table,
                    current_index_of_footer_legal_act=index_of_footer_document,
                    current_index_of_margin_legal_act=index_of_margin_document,
                    multiline_llm_extractor=multiline_text_extractor
                )
                index_of_document += legal_act_page.len_paragraph_lines
                index_of_table += len(legal_act_page.get_table_lines)
                index_of_footer_document += legal_act_page.len_footer_lines
                index_of_margin_document += legal_act_page.len_margin_lines
                pages.append(legal_act_page)

                # See Github issue https://github.com/jsvine/pdfplumber/issues/193
                # clearing cache of pages for pdfplumber, to overcome memory leaks
                page.flush_cache()
                page.get_textmap.cache_clear()
                # del paragraph_areas
                # del margin_areas
                # del margin
                del page
        finally:
            pdf.close()
            gc.collect()

    r = 4
    w =4
    return pages


if __name__ == '__main__':
    pdf_s3_path = 's3://datalake-bucket-123/stages/documents-to-download-pdfs/74d77c70-0e2f-47d1-a98f-b56b4c181120/D20110696Lj.pdf'
    bucket, key = extract_bucket_and_key(pdf_s3_path)
    pdf_content = io.BytesIO(
        boto3.client('s3', region_name=AWS_REGION).get_object(Bucket=bucket, Key=key)['Body'].read())
    # page_regions_paragraphs, page_regions_margin_notes, page_regions_footer_notes, attachments_regions = declare_part_of_legal_act("DU/2001/639", pdf_content)
    row_with_details: EstablishLegalWithTablesResult = EstablishLegalWithTablesResult \
        .from_dict(
        get_mongodb_collection(
            db_name="preprocessing_legal_acts_texts",
            collection_name="legal_act_boundaries_with_tables_stage"
        ).find_one(
            {
                "general_info.ELI": 'DU/2011/696',
                # "invoke_id": row[DAG_TABLE_ID]
                "invoke_id": '1da9d10e-3468-42dc-adb4-de30beff2960'
            },
            {"_id": 0}
        )
    )

    create_pages(row_with_details.page_regions, pdf_content)
    s = 4
