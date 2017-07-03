DROP table if EXISTS public.grids_amman_temp;
CREATE  TABLE public.grids_amman_temp (cell_id serial not null primary key);
SELECT addgeometrycolumn('public', 'grids_amman_temp','cell', 0, 'POLYGON', 2);

DROP FUNCTION genhexagons(float, float, float, float, float);
CREATE OR REPLACE FUNCTION genhexagons(width float, xmin float,ymin  float,xmax float,ymax float  )
RETURNS float AS $total$
declare
    b float :=width/2;
    a float :=b/2; --sin(30)=.5
    c float :=2*a;
    height float := 2*a+c;  --1.1547*width;
    ncol float :=ceil(abs(xmax-xmin)/width);
    nrow float :=ceil(abs(ymax-ymin)/width);

    polygon_string varchar := 'POLYGON((' ||
                                        0 || ' ' || 0     || ' , ' ||
                                        b || ' ' || a     || ' , ' ||
                                        b || ' ' || a+c   || ' , ' ||
                                        0 || ' ' || a+c+a || ' , ' ||
                                     -1*b || ' ' || a+c   || ' , ' ||
                                     -1*b || ' ' || a     || ' , ' ||
                                        0 || ' ' || 0     ||
                                '))';
BEGIN
    INSERT INTO public.grids_amman_temp (cell) SELECT st_translate(cell, x_series*(2*a+c)+xmin, y_series*(2*(a +c))+ymin)
    from generate_series(0, ncol::int , 1) as x_series,
    generate_series(0, nrow::int,1 ) as y_series,
    (
       SELECT polygon_string::geometry as cell
       UNION
       SELECT ST_Translate(polygon_string::geometry, b , a+c)  as cell
    ) as two_hex;
    ALTER TABLE public.grids_amman_temp
    ALTER COLUMN cell TYPE geometry(Polygon, 32236)
    USING ST_SetSRID(cell,32236);
    RETURN NULL;
END;
$total$ LANGUAGE plpgsql;


WITH buffer_prj AS (
SELECT st_transform(geom, 32236) AS geom
FROM raw.amman),
geom_bbox as (
   SELECT
        250 as width,
        ST_XMin(geom) as xmin,
        ST_YMin(geom) as ymin,
        ST_XMax(geom) as xmax,
        ST_YMax(geom) as ymax
FROM buffer_prj
GROUP BY geom)
SELECT genhexagons(width,xmin,ymin,xmax,ymax)
FROM geom_bbox;


DROP TABLE grids.amman_grid_250;
CREATE TABLE grids.amman_grid_250 AS (
WITH buffer_prj AS (
SELECT st_transform(geom, 32236) AS geom
FROM raw.amman)
SELECT grid.* 
FROM public.grids_amman_temp AS grid
JOIN buffer_prj
ON st_intersects(cell, geom)
);
