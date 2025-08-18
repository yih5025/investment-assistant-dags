INSERT INTO x_user_profiles (
    username,
    user_id,
    display_name,
    category
) VALUES (
    %(username)s,
    %(user_id)s,
    %(display_name)s,
    %(category)s
)
ON CONFLICT (username)
DO UPDATE SET
    user_id = EXCLUDED.user_id,
    display_name = EXCLUDED.display_name,
    category = EXCLUDED.category;