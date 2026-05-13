def recommend(user_id, session):
    query = """
        SELECT item_id, score
        FROM user_interactions
        WHERE user_id=%s
    """

    rows = session.execute(query, (user_id,))

    results = []
    for r in rows:
        # simple ranking logic
        score = r.score

        results.append({
            "item_id": r.item_id,
            "score": score
        })

    # sort descending
    results.sort(key=lambda x: x["score"], reverse=True)

    return results[:10]