def recommend(user_id, session, es):

    # 1. user history
    rows = session.execute("""
        SELECT item_id, score FROM user_interactions
        WHERE user_id=%s
    """, (user_id,))

    user_items = {r.item_id for r in rows}

    # 2. ES candidates
    res = es.search(
        index="items",
        query={"match_all": {}},
        size=20
    )

    candidates = res["hits"]["hits"]

    results = []

    for c in candidates:
        item = c["_source"]
        item_id = item["id"]

        # skip already seen
        if item_id in user_items:
            continue

        # fake scoring (you will improve later)
        score = c["_score"]

        results.append({
            "item_id": item_id,
            "score": score
        })

    results.sort(key=lambda x: x["score"], reverse=True)

    return results[:10]