/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
SELECT c.name            category,
       COUNT(fc.film_id) films
FROM category c
         INNER JOIN film_category fc ON fc.category_id = c.category_id
GROUP BY name
ORDER BY films DESC;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
-- Must group by actor_id, because there are actors with a different id, but with the same names (first and last)
SELECT actor
FROM (SELECT a.actor_id,
             CONCAT(a.first_name, ' ', a.last_name) actor,
             COUNT(i.inventory_id)                  rentals
      FROM actor a
               INNER JOIN film_actor fa ON fa.actor_id = a.actor_id
               INNER JOIN inventory i ON i.film_id = fa.film_id
               INNER JOIN rental r ON r.inventory_id = i.inventory_id
      GROUP BY a.actor_id, actor
      ORDER BY rentals DESC LIMIT 10) temp;



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
-- SQL code goes here...
SELECT category
FROM (SELECT c.name        category,
             SUM(p.amount) total_amount
      FROM category c
               INNER JOIN film_category fc ON fc.category_id = c.category_id
               INNER JOIN inventory i ON i.film_id = fc.film_id
               INNER JOIN rental r ON r.inventory_id = i.inventory_id
               INNER JOIN payment p ON p.rental_id = r.rental_id
      GROUP BY category
      ORDER BY total_amount DESC LIMIT 1) temp;



/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
-- SQL code goes here...
SELECT f.title
FROM film f
         LEFT JOIN inventory i ON i.film_id = f.film_id
WHERE i.inventory_id IS NULL
ORDER BY title;

/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
-- SQL code goes here...

-- Must group by actor_id, because there are actors with a different id, but with the same name,
-- so different actors in fact
SELECT actor
FROM (SELECT a.actor_id,
             CONCAT(a.first_name, ' ', a.last_name) actor,
             COUNT(fa.film_id)                      films
      FROM actor a
               INNER JOIN film_actor fa ON fa.actor_id = a.actor_id
               INNER JOIN film_category fc ON fa.film_id = fc.film_id
               INNER JOIN category c ON c.category_id = fc.category_id
      WHERE c.name = 'Children'
      GROUP BY a.actor_id, actor
      ORDER BY films DESC, actor LIMIT 3) temp;
