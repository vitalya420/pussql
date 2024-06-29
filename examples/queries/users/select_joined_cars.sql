SELECT users.id AS users_id, users.name AS users_name, cars.id AS car_id, cars.name AS car_name
FROM users
LEFT JOIN cars ON users.id = cars.user_id;