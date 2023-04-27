SELECT A.nombre AS Nombre, A.apellido AS Apellido, A.edad AS Edad, A.promedio AS Promedio
FROM Alumnos A
WHERE A.id = ?
AND A.edad > ?
AND A.promedio < ?
AND A.nombre LIKE ?
AND A.apellido LIKE ?
