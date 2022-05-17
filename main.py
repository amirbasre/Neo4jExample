from neo4j import GraphDatabase, graph
import os

DB_URL = "bolt://localhost:7687"
DB_USER = "neo4j"
DB_PASS = "123"

# Class to describe using neo4j function
class DirectoryGraph:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    @classmethod
    def create_directory_item(cls, tx, path, count):
        tx.run("MERGE (:DirectoryItem {path: $path, fileCount: $count})", path=path, count=count)

    @classmethod
    def create_file_item(cls, tx, path, extension, size):
        tx.run("MERGE (:FileItem {path: $path, extension: $extension, size: $size})", path=path, extension=extension, size=size)

    @classmethod
    def create_connection_between_folder_to_folder(cls, tx, path_a, path_b):
        tx.run("MATCH (a:DirectoryItem {path: $path_a}) "
               "MATCH (b:DirectoryItem {path: $path_b}) "
               "MERGE (a)-[:HAS_CHILD]->(b)",
               path_a=path_a, path_b=path_b)

    @classmethod
    def create_connection_between_folder_to_file(cls, tx, path_a, path_b):
        tx.run("MATCH (a:DirectoryItem {path: $path_a}) "
                "MATCH (b:FileItem {path: $path_b}) "
                "MERGE (a)-[:HAS_CHILD]->(b)",
                path_a=path_a, path_b=path_b)

    @classmethod
    def max_sub_directories(cls, tx):
        return_value = []
        result_from_query = tx.run("MATCH (a: DirectoryItem)-[:HAS_CHILD*]->(z:DirectoryItem) "
                                    "with a as first_directory, count(z) as subs "
                                    "order by subs desc limit 1 "
                                    "return first_directory.path")
        for record in result_from_query:
            return_value.append(record['first_directory.path'])
        return return_value

    @classmethod
    def find_exe(cls, tx):
        return_value = []
        result_from_query = tx.run("MATCH (a: DirectoryItem)-[:HAS_CHILD*1..]->(z:FileItem)"
                                    "WHERE z.extension='.exe'"
                                    "RETURN distinct a.path")
        for record in result_from_query:
            return_value.append(record['a.path'])
        return return_value

    @classmethod
    def find_root(cls, tx):
        return_value = []
        result_from_query = tx.run("MATCH (a: DirectoryItem)-[HAS_CHILD*1..]->(z:DirectoryItem)"
                                    "where not (a)<-[]-()"
                                    "RETURN a.path, count(z) as count_times")
        for record in result_from_query:
            return_value.append({"path": record['a.path'],"count": record['count_times']})
        return return_value

    @classmethod
    def find_exactly_3_empty_subdirectory(cls, tx):
        return_value = []
        result_from_query = tx.run("MATCH(a: DirectoryItem)-[: HAS_CHILD]->(z:DirectoryItem) "
                                    "where a.fileCount = 0 and not (z) - [:HAS_CHILD]->(:DirectoryItem) "
                                    "with a, count(a) as count_child "
                                    "where count_child = 3 "
                                    "return a.path")
        for record in result_from_query:
            return_value.append(record['a.path'])
        return return_value

    @classmethod
    def find_equal_files(cls, tx):
        return_value = []
        result_from_query = tx.run("MATCH(a: FileItem) "
                                    "MATCH(b: FileItem) "
                                    "WHERE a.path <> b.path and split(a.path,'\\\\')[-1]=split(b.path,'\\\\')[-1] and "
                                    "size(split(split(a.path,'\\\\')[-1],'.')[0]) >= 4 "
                                    "with a,b "
                                    "match (a)-[r:HAS_CHILD*]-(b) "
                                    "with a, count(r) as cnt where cnt >1 "
                                    "return a.path, cnt")
        for record in result_from_query:
            return_value.append({"path": record['a.path'],"count": record['cnt']})
        return return_value

# Class that represents the function to create and query the graph
class DirectoryMainFuncs:

    @classmethod
    def count_num_of_files(self, path_to_check):
        return len([file for file in os.listdir(path_to_check) if
                    os.path.isfile(os.path.join(path_to_check, file))])

    @classmethod
    def add_files_to_grpah(self, path_to_add, graph):
        import pathlib

        with graph.driver.session() as session:
            # Handle the parent of the directory
            parent_path = os.path.abspath(path_to_add)
            number_of_files_in_dir = self.count_num_of_files(parent_path)
            session.write_transaction(graph.create_directory_item, parent_path, number_of_files_in_dir)
            for root, dirs, files in os.walk(path_to_add):
                path = root.split(os.sep)
                print((len(path) - 1) * '---', os.path.basename(root))
                # Handle the dirs of the directory
                for dir in dirs:
                    path_of_current_dir = os.path.join(root, dir)
                    number_of_files_in_dir = self.count_num_of_files(path_of_current_dir)
                    session.write_transaction(graph.create_directory_item,
                                              path_of_current_dir,
                                              number_of_files_in_dir)
                    session.write_transaction(graph.create_connection_between_folder_to_folder,
                                              os.path.abspath(root),
                                              path_of_current_dir)
                # Handle the files of the directory
                for file in files:
                    print(len(path) * '---', file)
                    path_of_current_file = os.path.join(root, file)
                    session.write_transaction(graph.create_file_item,
                                              path_of_current_file,
                                              pathlib.Path(path_of_current_file).suffix,
                                              os.path.getsize(path_of_current_file))
                    session.write_transaction(graph.create_connection_between_folder_to_file,
                                              os.path.abspath(root),
                                              path_of_current_file)
    def run_queries(graph):
        with graph.driver.session() as session:
            print("")
            print("-------------question 1------------")
            print(session.read_transaction(graph.max_sub_directories))
            print("-------------question 2------------")
            print(session.read_transaction(graph.find_exe))
            print("-------------question 3------------")
            print(session.read_transaction(graph.find_root))
            print("-------------question 4------------")
            print(session.read_transaction(graph.find_exactly_3_empty_subdirectory))
            print("-------------question 5------------")
            print(session.read_transaction(graph.find_equal_files))

def get_input():
    print("Please select directory to use: (Caps lock does not matter)")
    path_to_check = input()
    while os.path.isfile(path_to_check) or not os.path.exists(path_to_check):
        print("Not a path, Please select directory to use: (example C:\drivers)")
        path_to_check = input()
    return path_to_check


if __name__ == '__main__':
    graph = DirectoryGraph(DB_URL, DB_USER, DB_PASS)
    DirectoryMainFuncs.add_files_to_grpah(get_input(), graph)
    DirectoryMainFuncs.run_queries(graph)
