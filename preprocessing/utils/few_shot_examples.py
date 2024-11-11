from langchain_core.pydantic_v1 import BaseModel, Field

from preprocessing.utils.llm_chain_type import LLMChainType


class CreateAnnotationOfTable(BaseModel):
    text_page: str


def get_few_shot_examples(chain_type: LLMChainType):
    if chain_type == LLMChainType.ADD_ANNOTATION_OF_TABLE_CHAIN:
        return [
            {
                "input": '''
        opakowaniu albo liczbę jednostkowych wyrobów medycznych, albo liczbę jednostek wyrobu medycznego.]
<4. Ustala się urzędową marżę detaliczną naliczaną od ceny hurtowej brutto Nowe brzmienie leku, środka spożywczego specjalnego przeznaczenia żywieniowego albo wyrobu ust. 4 w art. 7 wejdzie w życie z dn. 1.01.2025 r. (Dz. U. z 2023 r. poz. 1938).
od do zasada marży
– 10,00 zł 0,45 zł + 35% * x
10,01 zł 20,00 zł 3,95 zł + 25% * (x – 10,00 zł)
20,01 zł 40,00 zł 6,45 zł + 15% * (x – 20,00 zł)
40,01 zł 80,00 zł 9,45 zł + 10% * (x – 40,00 zł)
80,01 zł 160,00 zł 13,45 zł + 5% * (x – 80,00 zł)
160,01 zł 640,00 zł 17,45 zł + 2,75% * (x – 160,00 zł)
640,01 zł 30,65 zł + 2% * (x – 640,00 zł)
– gdzie x oznacza cenę hurtową brutto leku, środka spożywczego specjalnego przeznaczenia żywieniowego albo wyrobu medycznego stanowiącego podstawę limitu, uwzględniającą liczbę DDD leku, jednostek środka spożywczego specjalnego przeznaczenia żywieniowego w''',
                "output": CreateAnnotationOfTable(text_page='''
        opakowaniu albo liczbę jednostkowych wyrobów medycznych, albo liczbę jednostek wyrobu medycznego.]
<4. Ustala się urzędową marżę detaliczną naliczaną od ceny hurtowej brutto Nowe brzmienie leku, środka spożywczego specjalnego przeznaczenia żywieniowego albo wyrobu ust. 4 w art. 7 wejdzie w życie z dn. 1.01.2025 r. (Dz. U. z 2023 r. poz. 1938).
/table_start/
od do zasada marży
– 10,00 zł 0,45 zł + 35% * x
10,01 zł 20,00 zł 3,95 zł + 25% * (x – 10,00 zł)
20,01 zł 40,00 zł 6,45 zł + 15% * (x – 20,00 zł)
40,01 zł 80,00 zł 9,45 zł + 10% * (x – 40,00 zł)
80,01 zł 160,00 zł 13,45 zł + 5% * (x – 80,00 zł)
160,01 zł 640,00 zł 17,45 zł + 2,75% * (x – 160,00 zł)
640,01 zł 30,65 zł + 2% * (x – 640,00 zł)
– gdzie x oznacza cenę hurtową brutto leku, środka spożywczego specjalnego przeznaczenia żywieniowego albo wyrobu medycznego stanowiącego podstawę limitu, uwzględniającą liczbę DDD leku, jednostek środka spożywczego specjalnego przeznaczenia żywieniowego w''')
            },
            {
                "input": '''
        niniejszej ustawy, pomniejszona o marżę hurtową w wysokości 8,91% liczoną od ceny urzędowej hurtowej oraz marżę detaliczną w wysokości:
Cena hurtowa w złotych Marża detaliczna liczona od ceny hurtowej
0–3,60 40%
3,61–4,80 1,44 zł
4,81–6,50 30%
6,51–9,75 1,95 zł
9,76–14,00 20%
14,01–15,55 2,80 zł
15,56–30,00 18%
30,01–33,75 5,40 zł
33,76–50,00 16%
50,01–66,67 8,00 zł
66,68–100,00 12%
powyżej 100,00 12,00 zł
Negocjacje przeprowadza Komisja. Do negocjacji stosuje się art. 18 ust. 3, art. 19 ust. 1 i 2 pkt 2–7, art. 20 i art. 22 oraz przepisy wykonawcze wydane na podstawie art. 23.
        ''',
                "output": CreateAnnotationOfTable(text_page='''
        niniejszej ustawy, pomniejszona o marżę hurtową w wysokości 8,91% liczoną od ceny urzędowej hurtowej oraz marżę detaliczną w wysokości:
/table_start/
Cena hurtowa w złotych Marża detaliczna liczona od ceny hurtowej
0–3,60 40%
3,61–4,80 1,44 zł
4,81–6,50 30%
6,51–9,75 1,95 zł
9,76–14,00 20%
14,01–15,55 2,80 zł
15,56–30,00 18%
30,01–33,75 5,40 zł
33,76–50,00 16%
50,01–66,67 8,00 zł
66,68–100,00 12%
powyżej 100,00 12,00 zł
Negocjacje przeprowadza Komisja. Do negocjacji stosuje się art. 18 ust. 3, art. 19 ust. 1 i 2 pkt 2–7, art. 20 i art. 22 oraz przepisy wykonawcze wydane na podstawie art. 23.
        ''')
            }
        ]


class TypeOfAttachment(BaseModel):
    type_of_attachment: str = Field(...,
                                    enum=[
                                        'wzór_dokumentu',
                                        'zawiera_tabele',
                                        'wzory_do_obliczeń',
                                        'inne'
                                    ],
                                    description= "Rodzaj załącznika na podstawie jego treści. Wybierz najbardziej odpowiednią kategorię spośród poniższych opcji:\n\n"
                                                 "- **wzór dokumentu**: Użyj tej kategorii, gdy załącznik przedstawia wzór lub szablon dokumentu, który posiada "
                                                 "strukturalne sekcje, pola do uzupełnienia lub inne elementy typowe dla dokumentów (np. umowy, formularze, oficjalne dokumenty).\n\n"
                                                 "- **tabela**: Wybierz tę kategorię, jeśli załącznik zawiera dane w formie tabeli, zorganizowane w wiersze i kolumny "
                                                 "do porównania, zestawienia lub klasyfikacji informacji. Tabele często zawierają opisy kolumn i wierszy, mogą zawierać liczby lub dane kategoryczne. Nawet jeśli poza tabela jest tekst na stronie wyżej czy niżej i tak sklasyfikuj to jako tabele\n\n"
                                                 "- **wzory do obliczeń**: Wybierz tę kategorię dla załączników, które zawierają głównie wzory matematyczne lub szablony obliczeń. "
                                                 "Takie załączniki zawierają symbole, równania lub kroki przeznaczone do celów obliczeniowych, np. wzory z zakresu fizyki lub finansów.\n\n"
                                                 "- **inne**: Użyj tej kategorii dla załączników, które nie pasują do żadnej z powyższych kategorii. Kategoria ta służy jako ogólna opcja dla nietypowych rodzajów załączników, które nie są szablonem dokumentu, tabelą ani zestawem wzorów."
                                    )