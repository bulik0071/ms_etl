import argparse
import sqlite3
from sqlite3 import Error
from zipfile36 import ZipFile
import pathlib

sql_create_triplets_sample_20p = """ CREATE TABLE IF NOT EXISTS triplets_sample_20p(
                                        listener_id text ,
                                        track_id text NOT NULL,
                                        play_date text
                                    ); """

sql_create_unique_tracks = """CREATE TABLE IF NOT EXISTS unique_tracks(
                                    performance_id text ,
                                    track_id text NOT NULL,
                                    artist text NOT NULL,
                                    title text NOT NULL
                                );"""


current_path=pathlib.Path(__file__).parent.resolve()
print(f"Current path: {pathlib.Path(__file__).parent.resolve()}")
def handle_db(db_file,tracks,triplets):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        print(f"Connection to db successful. SQLITE Version: {sqlite3.version}")
        c.execute(sql_create_triplets_sample_20p)
        c.execute(sql_create_unique_tracks)
        print("Tables created")
        c.executemany('INSERT INTO unique_tracks VALUES(?,?,?,?);',tracks);
        print('Tracks inserted')
        c.executemany('INSERT INTO triplets_sample_20p VALUES(?,?,?);',triplets);
        print('Triplets inserted')
        conn.commit()
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
parser = argparse.ArgumentParser()
parser.add_argument('--db_path', type=str, required=True)
parser.add_argument('--file_path', type=str, required=True)
args = parser.parse_args()
print(f"Path to db: {args.db_path}, path to source file: {args.file_path}")

with ZipFile(args.file_path, 'r') as zObject:
    print(f"Extracting source file: {args.file_path}")
    zObject.extractall(path=current_path)
    print(f'Extraction successfully')
with ZipFile(str(current_path)+r"\ETL\unique_tracks.zip", 'r') as zObject:
    print(f"Extracting file:unique_tracks.zip")
    zObject.extractall(path=current_path)
    print(f'Extraction successfully')
with ZipFile(str(current_path)+r"\ETL\triplets_sample_20p.zip", 'r') as zObject:
    print(f"Extracting file: triplets_sample_20p.zip")
    zObject.extractall(path=current_path)
    print(f'Extraction successfully')
triplets_to_db=[]
tracks_to_db=[]
triplets_file=open('triplets_sample_20p.txt', errors="ignore").read().splitlines()
for triplet in triplets_file:
    splitted_triplet=triplet.split('<SEP>')
    triplets_to_db.append((splitted_triplet[0],splitted_triplet[1],splitted_triplet[2]))


tracks_file=open('unique_tracks.txt', errors="ignore").read().splitlines()
for track in tracks_file:
    splitted_track=track.split('<SEP>')
    try:
        tracks_to_db.append((splitted_track[0],splitted_track[1],splitted_track[2],splitted_track[3]))
    except:
        pass

handle_db(args.db_path,tracks_to_db,triplets_to_db)

if __name__=="__main__":
    try:
        conn = sqlite3.connect(args.db_path)
        c = conn.cursor()
        c.execute("SELECT track_id, COUNT(*) FROM triplets_sample_20p GROUP BY track_id ORDER BY COUNT(*) DESC LIMIT 5;")
        rows = c.fetchall()
        top5=[]
        overall={}
        for row in rows:
            query="SELECT title FROM unique_tracks WHERE track_id='"+str(row[0])+"';"
            c.execute(query)
            title=c.fetchone()
            top5.append((title[0])+':'+str(row[1]))
        print(f"TOP5:\n 1st: {top5[0]}\n 2nd: {top5[1]}\n 3rd: {top5[2]}\n 4th: {top5[3]}\n 5th: {top5[4]}")
        c.execute("SELECT track_id, COUNT(*) FROM triplets_sample_20p GROUP BY track_id ORDER BY COUNT(*) DESC;")
        rows = c.fetchall()
        for row in rows:
            query="SELECT artist FROM unique_tracks WHERE track_id='"+str(row[0])+"';"
            c.execute(query)
            artist=c.fetchone()
            if overall.get(artist[0]) is None:
                overall[artist[0]]=row[1]
            else:
                act_value=overall.get(artist[0])
                overall[artist[0]]=row[1]+act_value
        print(max(overall, key=overall.get))

    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()
