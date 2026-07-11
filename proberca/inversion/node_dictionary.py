"""Sparse node self-fault dictionary U = [I; 0]."""

from scipy import sparse

from .contracts import NodeVariableRef


def build_node_dictionary(node_ids: list[str], edge_count: int):
    node_count = len(node_ids)
    matrix = sparse.vstack(
        (sparse.identity(node_count, format="csr", dtype=float),
         sparse.csr_matrix((edge_count, node_count), dtype=float)),
        format="csr",
    )
    refs = [NodeVariableRef(index, node_id, index) for index, node_id in enumerate(node_ids)]
    return matrix, refs
