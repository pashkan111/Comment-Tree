import abc
import asyncio
from dataclasses import asdict, dataclass

import asyncpg
import fastapi


@dataclass(frozen=True, slots=True)
class Comment:
    id: int | None = None
    text: str | None = None
    parent_id: int | None = None


@dataclass
class Comments:
    comments: list[Comment]


class DBConnector(abc.ABC):
    conn_url: str

    def connect(self):
        ...


class PostgresConnector:
    conn_url: str

    def __init__(
        self, db_name, db_pass, db_port, db_user, db_host
        ):
        self.conn_url = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
    
    async def _connect(self) -> asyncpg.Connection:
        conn = await asyncpg.connect(self.conn_url)
        return conn
    
    async def execute(self, query: str) -> list[asyncpg.Record]:
        conn = await self._connect()
        result = await conn.fetch(query)
        await conn.close()
        return result


class CommentParser:
    def create_comment_model(
        self, records: list[asyncpg.Record]
    ) -> Comments:
        comments = []
        for record in records:
            values = list(record.values())
            comments.append(Comment(*values))
        return Comments(comments)


class CommentManager:
    db_conn: PostgresConnector
    comment_parser: CommentParser

    def __init__(self, db_conn, comment_parser):
        self.db_conn = db_conn
        self.comment_parser = comment_parser

    async def get_comments(self) -> Comments:
        query = "SELECT * FROM comments ORDER BY parent_id NULLS FIRST"
        comments = await self.db_conn.execute(query)
        comments_models = self.comment_parser.create_comment_model(comments)
        return comments_models

    async def insert_comment(self, comment: Comment):
        query = "INSERT INTO comments(id, text, parent_id)"\
            f"values (DEFAULT, '{comment.text}', {comment.parent_id})"\
            "returning *"
        created_comment = await self.db_conn.execute(query)
        comment_model = self.comment_parser.create_comment_model(created_comment)

        return comment_model


connector = PostgresConnector('movies', 'password', 5429, 'postgres', 'localhost')
comment_parser = CommentParser()
comment_manager = CommentManager(connector, comment_parser)


# app = fastapi.FastAPI()


# @app.get('/comments')
# async def get_comments():
#     comments = await comment_manager.get_comments()
#     return asdict(comments)


# @app.post('/create-comment')
# async def create_comment(data: Comment):
#     print(data.text)
#     comment = await comment_manager.insert_comment(data)
#     return asdict(comment)



def get_dict_of_child_comment(
    comment: Comment, tree: dict[Comment, dict], cached: dict[int, Comment]
):
    parent_comment = cached.get(comment.parent_id)
    dict_of_child_comments = tree.get(parent_comment)
    if dict_of_child_comments is None:
        dict_of_child_comments = get_dict_of_child_comment(parent_comment, tree, cached)
        return dict_of_child_comments[parent_comment]
    return dict_of_child_comments


def sort_comments(comments: list[Comment]) -> list[Comment]:
    sorted_comments = sorted(comments, key=lambda comment: comment.parent_id)
    return sorted_comments


def build_tree(comments_list: Comments, sorted=True):
    # Comments must be sorted by parent id. Roots comments first
    tree = {}
    cached = {}
    comments = comments_list.comments
    if sorted is False:
        comments = sort_comments(comments)
    for comment in comments:
        if not comment.parent_id:
            tree.setdefault(comment, {})
            cached.setdefault(comment.id, comment)
        else:
            dict_of_child_comment = get_dict_of_child_comment(comment, tree, cached)
            dict_of_child_comment.setdefault(comment, {})
            cached.setdefault(comment.id, comment)
    return tree


comments = asyncio.run(comment_manager.get_comments())
print(comments)
tree = build_tree(comments)


def printer(tree: dict[Comment, dict], level=0):
    for comment, children in tree.items():
        if not comment.parent_id: 
            level = 0
        print(level * ' ', comment.text)
        # print(level * ' ', comment)
        if len(children) == 0:
            continue

        printer(children, level+4)


printer(tree)
