---------------------
--   Problem 1 :   --
---------------------
.shell echo "Problem 1:"
-- write sql below --
select distinct name
from Person
order by name asc;

---------------------
--   Problem 2 :   --
---------------------
.shell echo "\nProblem 2:"
-- write sql below --
select distinct name
from Person
where company = 'Horse Clan of the Plains'
order by name asc;

---------------------
--   Problem 3 :   --
---------------------
.shell echo "\nProblem 3:"
-- write sql below --
select distinct name
from Person p, Likes l
where p.ID = l.ID2
order by name asc;

---------------------
--   Problem 4 :   --
---------------------
.shell echo "\nProblem 4:"
-- write sql below --
select distinct name
from Person p
left outer join Likes l
on p.ID = l.ID2
where l.ID2 is null
order by name asc;

---------------------
--   Problem 5 :   --
---------------------
.shell echo "\nProblem 5:"
-- write sql below --
select count(company), company
from Person p
group by company;

---------------------
--   Problem 6 :   --
---------------------
.shell echo "\nProblem 6:"
-- write sql below --
select distinct count(company), company
from Person p
left outer join Likes l
on p.ID = l.ID2
where l.ID2 is null
group by company;

---------------------
--   Problem 7 :   --
---------------------
.shell echo "\nProblem 7:"
-- write sql below --
select name, company
from Person p
left outer join Friend f
on p.ID = f.ID1
group by name, company
having count(p.ID) > 3;

---------------------
--   Problem 8 :   --
---------------------
.shell echo "\nProblem 8:"
-- write sql below --
select distinct name, age
from Person p1
where age > (select age
from Person p2
left outer join Likes l
on p2.ID = l.ID1
where p1.ID = l.ID2)
group by name, age;

---------------------
--   Problem 9 :   --
---------------------
.shell echo "\nProblem 9:"
-- write sql below --
select p1.ID, p1.name, p2.ID, p2.name
from Likes l
join Person p1 on p1.ID = l.ID1
join Person p2 on p2.ID = l.ID2
where not exists (select f.ID1, f.ID2
from Friend f
where p1.ID = f.ID1
and p2.ID = f.ID2)
order by p1.ID asc, p2.ID asc;

---------------------
--  Extra Credit : --
---------------------
.shell echo "\nProblem 10:"
-- write sql below --


---------------------
