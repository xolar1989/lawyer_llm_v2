from typing import Dict


class CharLegalAct:

    ## TODO add num_of_page
    def __init__(self, x0: float, x1: float, bottom: float, top: float,
                 text: str, index_in_legal_act: int, page_number: int):
        self.x0 = x0
        self.x1 = x1
        self.bottom = bottom
        self.top = top
        self.text = text
        self.index_in_legal_act = index_in_legal_act
        self.page_number = page_number

    @classmethod
    def get_height_from_dict(cls, char: Dict) -> float:
        return round(char["height"], 1)