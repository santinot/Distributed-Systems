# SistemiDistribuiti
Utilizzare Ray con algoritmo di MapReduce per andare a fare una ricerca riguardo record contenuti in un file JSON di almeno 100mb ( aventi coppie chiave-valore DATI SERIALIZZATI) contenente username,password e altre informazioni riguardanti un servizio di streaming tipo Netflix (es. genere film, titolo, ecc.). Effettuare dei test prima su singole core, poi multicore e poi cluster.
Creare una funzione che genera questi dati e che si salvi in formato json all'interno di un file.

fare funzione di mapping che divide prima per chiave generale, e poi in modo specifico per i film, associadno poi un valore numerico che incrementa ogni volta che si ha l'occorrenza di un film.
fare funzione di shuffling che prende come input la lista di dizionari ritornata dalla funzione di mapping, divida le occorrenze uguali e sommi i valori ad esse assocaiti in modo da ritornare un occorrenza unica chiave-valore, quest'ultimo dato dalla somma di tutti quelli registrati precedentemente. 
