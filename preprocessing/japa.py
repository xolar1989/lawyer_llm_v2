import re

import pandas as pd

# Example of stored data
df_stored = pd.DataFrame({
    'ID': [1, 2, 3,4],
    'Name': ['Alice', 'Bob', 'Charlie','David'],
    'Age': [25, 30, 35,40],
    'City': ['New York', 'Los Angeles', 'Chicago','San Francisco']
})

# Example of new data with potential changes
df_new = pd.DataFrame({
    'ID': [1, 2, 3],
    'Name': ['Alice', 'Bob', 'Charles'],  # Charlie has been changed to Charles
    'Age': [25, 31, 35],  # Bob's age has been updated
    'City': ['New York', 'Los Angeles', 'San Francisco']  # Charlie has moved
})

# Merge on the ID column
df_combined = pd.merge(df_stored, df_new, on='ID', suffixes=('_stored', '_new'))

# Identify where the properties (columns) differ
for column in ['Name', 'Age', 'City']:
    df_combined[f'{column}_changed'] = df_combined[f'{column}_stored'] != df_combined[f'{column}_new']

# Display the rows with changes
changes = df_combined[df_combined[['Name_changed', 'Age_changed', 'City_changed']].any(axis=1)]
print(changes)


ss = '''
Liczba stron : 12 Data : 2015-01-08 Nazwa pliku : D20060550LJ.DOCX
12
VII kadencja/druk
Załącznik
Wzór informacji o radzie pracowników
1. Nazwa pracodawcy ......................................................
2. Adres (siedziba lub miejsce zamieszkania) pracodawcy ....................................
3. Data utworzenia rady pracowników ..............................................
4. Liczba członków rady pracowników .............................................
5. Czy warunki informowania pracowników i przeprowadzania z nimi konsultacji:
1) zostały ustalone przez radę pracowników z pracodawcą, lub
2) obowiązują regulacje ustawowe ..........................................
6. Zakres ustaleń przyjętych przez radę pracowników z pracodawcą na podstawie
art. 5 ust. 1 i 2 ............................................
Objaśnienie:
Ad 4 - wskazać podstawę prawną, tj. art. 3 ust. 1 pkt 1, 2 lub 3

'''


text2 = '''
14
1890.TO
Załącznik nr 2
WZÓR
OŚWIADCZENIE
o zamiarze podjęcia lub o zmianie charakteru działalności gospodarczej prowadzonej
przez małżonka
Ja, niżej podpisany(a), ...............................................................................................................................................................
(imiona i nazwisko)
urodzony(a) .................................................................. w ........................................................................................................
zatrudniony(a) w .......................................................................................................................................................................
(miejsce zatrudnienia, stanowisko lub funkcja)
zamieszkały(a) w .......................................................................................................................................................................
nr dowodu osobistego ...............................................................................................................................................................
po zapoznaniu się z przepisami ustawy z dnia 30 listopada 2016 r. o statusie sędziów Trybunału Konstytucyjnego
(Dz. U. z 2018 r. poz. 1422), zgodnie z art. 13 ust. 1 tej ustawy oświadczam, że mój małżonek ...............................................
...................................................................................................................................................................................................
(imiona i nazwisko, w przypadku kobiet podać nazwisko panieńskie)
zamieszkały(a) w .......................................................................................................................................................................
1) podejmie działalność gospodarczą (przedmiot działalności, adres, data rozpoczęcia działalności) ....................................
...................................................................................................................................................................................................
...................................................................................................................................................................................................
2) zmienił(a) dotychczasową działalność gospodarczą (przedmiot działalności, adres) ..........................................................
...................................................................................................................................................................................................
...................................................................................................................................................................................................
na działalność (przedmiot działalności, adres) ..........................................................................................................................
...................................................................................................................................................................................................
...................................................................................................................................................................................................

'''

rrrrr = '\d{1,}\s*\.T\s*O'


w = re.search(r'L\s*i\s*c\s*z\s*b\s*a\s+s\s*t\s*r\s*o\s*n\s*:\s*\d+\s*D\s*a\s*t\s*a\s*:\s*\d{2,4}[-.]\d{2}[-.]\d{2,4}\s*N\s*a\s*z\s*w\s*a\s+p\s*l\s*i\s*k\s*u\s*:\s*[A-Za-z0-9/.]+\s*.+\s*[IXV]+\s*k\s*a\s*d\s*e\s*n\s*c\s*j\s*a\s*/d\s*r\s*u\s*k', ss)



miau = re.search(rrrrr, text2)
d = w.group(0)


ccc = 4
