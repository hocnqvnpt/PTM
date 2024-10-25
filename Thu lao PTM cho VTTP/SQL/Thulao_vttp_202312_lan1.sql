-- Chi tiet trong ky:      
drop table ct_bsc_ptm_202312_vttp;
create table ct_bsc_ptm_202312_vttp as
    select * from ct_bsc_ptm
    where nhom_tiepthi=2 
            and ((to_number(thang_ptm)>=202309  
                    and ((loaihd_id=1 and loaitb_id in (11,58,61)) or loaitb_id=20)
                    and (thang_tldg_dt='202312' or thang_tldg_dt is null))
                    or (loaitb_id=21 and thang_ptm='202312'));
    

                    
-- Tao bangluong_dongia_202312_vttp
drop table bangluong_dongia_202312_vttp;
create table bangluong_dongia_202312_vttp (
ma_nv               varchar(10),
ten_nv               varchar(50),
ma_vtcv            varchar(20),
ten_vtcv            varchar(100),
ma_to               varchar(10),
ten_to               varchar(100),
ma_pb         varchar(10),
ten_pb         varchar(100),

sl_brcd_chitl number,
sl_mytv_chitl number,
sl_vnpts_chitl number,
sl_vnptt_chitl number,
tong_sl_chitl number,

dt_brcd_chitl number,
dt_mytv_chitl number,
dt_vnpts_chitl number,
dt_vnptt_chitl number,
tong_dt_chitl number,

dt_brcd_chuachitl number,
dt_mytv_chuachitl number,
dt_vnpts_chuachitl number,
dt_vnptt_chuachitl number,
tong_dt_chuachitl number,

sl_brcd_chuachitl number,
sl_mytv_chuachitl number,
sl_vnpts_chuachitl number,
sl_vnptt_chuachitl number,
tong_sl_chuachitl number,

tl_brcd number,
tl_mytv number,
tl_vnpts number,
tl_vnptt number,
tong_tl number ,
nv_thang number 
);


insert into bangluong_dongia_202312_vttp
      (ma_nv,ten_nv,ma_vtcv,ten_vtcv,ma_pb,ten_pb,ma_to,ten_to,nv_thang )
SELECT ma_nv,ten_nv,ma_vtcv,ten_vtcv,ma_pb,ten_pb,ma_to,ten_to,202312
  FROM nhanvien_vttp a
  where exists(select 1 from ct_bsc_ptm_202312_vttp where manv_ptm=a.ma_nv)
        and thang=(select max(thang) from nhanvien_vttp where ma_nv=a.ma_nv);

    
-- tong hop:  
drop table th_tlptm_202312_vttp;
create table th_tlptm_202312_vttp as
select manv_ptm ma_nv, 
            (select ten_pb from nhanvien_vttp where thang=202312 and ma_nv=a.manv_ptm) ten_donvi,            
            sum(nvl(sl_brcd_chitl,0)) sl_brcd_chitl, 
            sum(nvl(sl_mytv_chitl,0)) sl_mytv_chitl,
            sum(nvl(sl_vnpts_chitl,0)) sl_vnpts_chitl,
            sum(nvl(sl_vnptt_chitl,0)) sl_vnptt_chitl,            
            
            sum(nvl(dt_brcd_chitl,0)) dt_brcd_chitl, 
            sum(nvl(dt_mytv_chitl,0)) dt_mytv_chitl,
            sum(nvl(dt_vnpts_chitl,0)) dt_vnpts_chitl,
            sum(nvl(dt_vnptt_chitl,0)) dt_vnptt_chitl,

            sum(nvl(tl_brcd,0)) tl_brcd, 
            sum(nvl(tl_mytv,0)) tl_mytv,
            sum(nvl(tl_vnpts,0)) tl_vnpts,
            sum(nvl(tl_vnptt,0)) tl_vnptt
            
from
(
    -- dthu + thu lao duoc tinh cdbr, mytv, vnpts
    select  manv_ptm, 
            count(case when loaitb_id in (11,58) then 1 end) sl_brcd_chitl,
            count(case when loaitb_id=61 then 1 end) sl_mytv_chitl,
            count(case when loaitb_id=20  then 1 end) sl_vnpts_chitl,
            0 as sl_vnptt_chitl,
            
            sum(case when loaitb_id in (11,58) then doanhthu_dongia_nvptm end) dt_brcd_chitl,
            sum(case when loaitb_id=61 then doanhthu_dongia_nvptm end) dt_mytv_chitl,
            sum(case when loaitb_id=20  then doanhthu_dongia_nvptm end) dt_vnpts_chitl,
            0 as dt_vnptt_chitl,

            sum(case when loaitb_id in (11,58) then luong_dongia_nvptm end) tl_brcd,
            sum(case when loaitb_id=61 then luong_dongia_nvptm end) tl_mytv,
            sum(case when loaitb_id=20  then luong_dongia_nvptm end) tl_vnpts,
            0 as tl_vnptt

    from ct_bsc_ptm_202312_vttp
    where thang_tldg_dt='202312' and loaitb_id!=21
    group by manv_ptm 
   
   union all 
    -- dthu goi vnptt 
    select  manv_ptm, 
            0 as sl_brcd_chitl,
            0 as sl_mytv_chitl,
            0 as sl_vnpts_chitl,
            0 as sl_vnptt_chitl,
            
            0 as dt_brcd_chitl,
            0 as dt_mytv_chitl,
            0 as dt_vnpts_chitl,
            sum(dthu_goi_goc) dt_vnptt_chitl,        -- doanhthu_kpi_nvhotro da bao gom dthu dnhm + dthu goi

            0 as tl_brcd,
            0 as tl_mytv,
            0 as tl_vnpts,          
            sum(luong_dongia_nvptm) tl_vnptt
    -- select *  
    from ct_bsc_ptm_202312_vttp
    where loaitb_id=21 and thang_ptm=202312 and thang_tldg_dt='202312'
    group by manv_ptm  

   
    union all
        -- dnhm brcd, mytv, vnpts, vnptt duoc tinh
    select  manv_ptm,
            0 as sl_brcd_chitl,
            0 as sl_mytv_chitl,
            0 as sl_vnpts_chitl,
            count(case when loaitb_id=21  then 1 end)  sl_vnptt_chitl,
            
            sum(case when loaitb_id in (11,58) then doanhthu_dongia_dnhm end) dt_brcd_chitl,
            sum(case when loaitb_id=61 then doanhthu_dongia_dnhm end) dt_mytv_chitl,
            sum(case when loaitb_id=20  then doanhthu_dongia_dnhm end) dt_vnpts_chitl,
            sum(case when loaitb_id=21 then tien_dnhm end ) dt_vnptt_chitl,

            sum(case when loaitb_id in (11,58) then luong_dongia_dnhm_nvptm end) tl_brcd,
            sum(case when loaitb_id=61 then luong_dongia_dnhm_nvptm end) tl_mytv,
            sum(case when loaitb_id=20 then luong_dongia_dnhm_nvptm end) tl_vnpts,
            sum(case when loaitb_id=21 then luong_dongia_dnhm_nvptm end)  as tl_vnptt
    -- select *
    from ct_bsc_ptm_202312_vttp
    where thang_tldg_dnhm='202312' 
    group by manv_ptm 
) a
where manv_ptm is not null
group by manv_ptm ;


-- sltb + dthu goi chua duoc tinh
drop table th_tlptm_202312_vttp_chuachitl;
create table th_tlptm_202312_vttp_chuachitl as
select manv_ptm ma_nv, 
            (select ten_pb from nhanvien_vttp where thang=202312 and ma_nv=a.manv_ptm) ten_donvi,
            sum(sl_brcd_chuachitl) sl_brcd_chuachitl, 
            sum(sl_mytv_chuachitl) sl_mytv_chuachitl,
            sum(sl_vnpts_chuachitl) sl_vnpts_chuachitl,
            sum(sl_vnptt_chuachitl) sl_vnptt_chuachitl,
            
            sum(nvl(dt_brcd_chuachitl,0)) dt_brcd_chuachitl, 
            sum(nvl(dt_mytv_chuachitl,0)) dt_mytv_chuachitl,
            sum(nvl(dt_vnpts_chuachitl,0)) dt_vnpts_chuachitl,
            sum(nvl(dt_vnptt_chuachitl,0)) dt_vnptt_chuachitl
from
    (select  manv_ptm, 
            count(case when loaitb_id in (11,58) then 1 end) sl_brcd_chuachitl,
            count(case when loaitb_id=61 then 1 end) sl_mytv_chuachitl,
            count(case when loaitb_id=20  then 1 end) sl_vnpts_chuachitl,
            0 as  sl_vnptt_chuachitl,
            
            sum(case when loaitb_id in (11,58) then doanhthu_dongia_nvptm end) dt_brcd_chuachitl,
            sum(case when loaitb_id=61 then doanhthu_dongia_nvptm end) dt_mytv_chuachitl,
            sum(case when loaitb_id=20  then doanhthu_dongia_nvptm end) dt_vnpts_chuachitl,
            0 as  dt_vnptt_chuachitl
            
    from ct_bsc_ptm_202312_vttp
    where loaitb_id in (11,58,61,20) and thang_tldg_dt is null 
    group by manv_ptm 
    
    union all
    select  manv_ptm, 
            0 as sl_brcd_chuachitl,
            0 as sl_mytv_chuachitl,
            0 as sl_vnpts_chuachitl,
            count(*) as sl_vnptt_chuachitl,
            
            0 as dt_brcd_chuachitl,
            0 as dt_mytv_chuachitl,
            0 as dt_vnpts_chuachitl,
            sum(tien_dnhm) dt_vnptt_chuachitl
    from ct_bsc_ptm_202312_vttp
    where loaitb_id=21 and thang_tldg_dnhm is null -- and nvl(doanhthu_kpi_nvhotro,0)>0
    group by manv_ptm
    ) a
group by manv_ptm;
    
    
-- ptm cdbr + gtgt +vnpts             
update bangluong_dongia_202312_vttp a
      set 
            sl_brcd_chitl='',
            sl_mytv_chitl='',
            sl_vnpts_chitl='',
            sl_vnptt_chitl='',
            tong_sl_chitl='',
            
            dt_brcd_chitl='',
            dt_mytv_chitl='',
            dt_vnpts_chitl='',
            dt_vnptt_chitl='',
            tong_dt_chitl='',

            tl_brcd='',
            tl_mytv='',
            tl_vnpts='',
            tl_vnptt='',
            tong_tl='',
            
            sl_brcd_chuachitl='',
            sl_mytv_chuachitl='',
            sl_vnpts_chuachitl='',
            sl_vnptt_chuachitl='',
            tong_sl_chuachitl='',

            dt_brcd_chuachitl='',
            dt_mytv_chuachitl='',
            dt_vnpts_chuachitl='',
            dt_vnptt_chuachitl='',
            tong_dt_chuachitl='';
    
            
update bangluong_dongia_202312_vttp a
            set ( sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, 
                    dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl,
                    tl_brcd, tl_mytv, tl_vnpts, tl_vnptt)
           =(select sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, 
                    dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl,
                    tl_brcd, tl_mytv, tl_vnpts, tl_vnptt
             from th_tlptm_202312_vttp
             where ma_nv=a.ma_nv)
    -- select * from bangluong_dongia_202312_vttp a
    where exists(select 1 from th_tlptm_202312_vttp where ma_nv=a.ma_nv);


update bangluong_dongia_202312_vttp a
            set (sl_brcd_chuachitl, sl_mytv_chuachitl, sl_vnpts_chuachitl, sl_vnptt_chuachitl,
                    dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl)
            =(select sl_brcd_chuachitl, sl_mytv_chuachitl, sl_vnpts_chuachitl, sl_vnptt_chuachitl,
                    dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl
              from th_tlptm_202312_vttp_chuachitl
              where ma_nv=a.ma_nv)
    -- select * from bangluong_dongia_202312_vttp a
    where exists(select 1 from th_tlptm_202312_vttp_chuachitl where ma_nv=a.ma_nv);
    
    
-- Tong:
update bangluong_dongia_202312_vttp set tong_dt_chitl='', tong_tl='' , tong_dt_chuachitl='',  tong_sl_chitl='', tong_sl_chuachitl='';
update bangluong_dongia_202312_vttp a
            set  tong_dt_chitl      = nvl(dt_brcd_chitl,0)+nvl(dt_mytv_chitl,0)+nvl(dt_vnpts_chitl,0)+nvl(dt_vnptt_chitl,0)    
                    ,tong_tl                = nvl(tl_brcd,0)+nvl(tl_mytv,0)+nvl(tl_vnpts,0)+nvl(tl_vnptt,0)
                    ,tong_dt_chuachitl = nvl(dt_brcd_chuachitl,0)+nvl(dt_mytv_chuachitl,0)+nvl(dt_vnpts_chuachitl,0)+nvl(dt_vnptt_chuachitl,0)
                    ,tong_sl_chitl      = nvl(sl_brcd_chitl,0)+nvl(sl_mytv_chitl,0)+nvl(sl_vnpts_chitl,0)+nvl(sl_vnptt_chitl,0)   
                    ,tong_sl_chuachitl = nvl(sl_brcd_chuachitl,0)+nvl(sl_mytv_chuachitl,0)+nvl(sl_vnpts_chuachitl,0)+nvl(sl_vnptt_chuachitl,0);


update bangluong_dongia_202312_vttp set dt_brcd_chitl='' where dt_brcd_chitl=0;
update bangluong_dongia_202312_vttp set dt_mytv_chitl='' where dt_mytv_chitl=0;
update bangluong_dongia_202312_vttp set dt_vnpts_chitl='' where dt_vnpts_chitl=0;
update bangluong_dongia_202312_vttp set dt_vnptt_chitl='' where dt_vnptt_chitl=0;
update bangluong_dongia_202312_vttp set tong_dt_chitl='' where tong_dt_chitl=0;

update bangluong_dongia_202312_vttp set tl_brcd='' where tl_brcd=0;
update bangluong_dongia_202312_vttp set tl_mytv='' where tl_mytv=0;
update bangluong_dongia_202312_vttp set tl_vnpts='' where tl_vnpts=0;
update bangluong_dongia_202312_vttp set tl_vnptt='' where tl_vnptt=0;
update bangluong_dongia_202312_vttp set tong_tl='' where tong_tl=0;
            
update bangluong_dongia_202312_vttp set dt_brcd_chuachitl='' where dt_brcd_chuachitl=0;
update bangluong_dongia_202312_vttp set dt_mytv_chuachitl='' where dt_mytv_chuachitl=0;
update bangluong_dongia_202312_vttp set dt_vnpts_chuachitl='' where dt_vnpts_chuachitl=0;
update bangluong_dongia_202312_vttp set dt_vnptt_chuachitl='' where dt_vnptt_chuachitl=0;
update bangluong_dongia_202312_vttp set tong_dt_chuachitl='' where tong_dt_chuachitl=0;
            
update bangluong_dongia_202312_vttp set sl_brcd_chitl='' where sl_brcd_chitl=0;
update bangluong_dongia_202312_vttp set sl_mytv_chitl='' where sl_mytv_chitl=0;
update bangluong_dongia_202312_vttp set sl_vnpts_chitl='' where sl_vnpts_chitl=0;
update bangluong_dongia_202312_vttp set sl_vnptt_chitl='' where sl_vnptt_chitl=0;
update bangluong_dongia_202312_vttp set tong_sl_chitl='' where tong_sl_chitl=0;

update bangluong_dongia_202312_vttp set sl_brcd_chuachitl='' where sl_brcd_chuachitl=0;
update bangluong_dongia_202312_vttp set sl_mytv_chuachitl='' where sl_mytv_chuachitl=0;
update bangluong_dongia_202312_vttp set sl_vnpts_chuachitl='' where sl_vnpts_chuachitl=0;
update bangluong_dongia_202312_vttp set sl_vnptt_chuachitl='' where sl_vnptt_chuachitl=0;
update bangluong_dongia_202312_vttp set tong_sl_chuachitl='' where tong_sl_chuachitl=0;


delete bangluong_dongia_202312_vttp
    -- select * from bangluong_dongia_202312_vttp
    where tong_tl is null and tong_dt_chuachitl is null and tong_sl_chitl is null and tong_sl_chuachitl is null ;
    
commit;
   
    
---- Kiem tra doanh thu 502027918
select 'bangluong_dongia_202312_vttp' , sum(tl_brcd), sum(tl_mytv) , sum(tl_vnpts), sum(tl_vnptt) from bangluong_dongia_202312_vttp 
union all
select 'ct_ptm - luong goi',
            sum(case when loaitb_id=58 then luong_dongia_nvptm end) tl_brcd,
            sum(case when loaitb_id=61 then luong_dongia_nvptm end) tl_mytv,
            sum(case when loaitb_id=20 then luong_dongia_nvptm end) tl_vnpts,
            sum(case when loaitb_id=21 then luong_dongia_nvptm end) tl_vnptt       
    from ct_bsc_ptm_202312_vttp
    where thang_tldg_dt='202312'

union all
select 'ct_ptm - luong goi',
            sum(case when loaitb_id=58 then luong_dongia_dnhm_nvptm end) tl_brcd,
            sum(case when loaitb_id=61 then luong_dongia_dnhm_nvptm end) tl_mytv,
            sum(case when loaitb_id=20 then luong_dongia_dnhm_nvptm end) tl_vnpts,
            sum(case when loaitb_id=21 then luong_dongia_dnhm_nvptm end) tl_vnptt       
    from ct_bsc_ptm_202312_vttp
    where thang_tldg_dnhm='202312';


       
-- Kiem tra so luong tb
select 'bangluong_dongia_202312_vttp_chi' , sum(tong_sl_chitl) tong_sl
 from bangluong_dongia_202312_vttp 
union all
select 'bangluong_dongia_202312_vttp_chi',  sum(tong_sl_chuachitl) tong_sl
 from bangluong_dongia_202312_vttp
union all 
select 'bangluong_dongia_202312_vttp_tong' , sum(tong_sl_chitl) + sum(tong_sl_chuachitl) tong_sl
 from bangluong_dongia_202312_vttp 
union all
select 'ct_bsc_ptm_202312_vttp', count(*) tong_sl
 from ct_bsc_ptm_202312_vttp;
                

-- Phu luc 1 - Tong hop doanh thu - thu lao PTM theo TTVT:
select * from (
select decode(MA_PB, 'HCM140000',1,'HCM150000',2,'HCM160000',3,'HCM170000',4,'HCM180000',5,'HCM190000',6,'HCM200000',7
                        ,'HCM210000',8,'HCM220000',9,'HCM030000',10,'HCM050000',11,'HCM080000',12,'HCM110000',13,'HCM100000',14
                        ,'HCM090000',15, 'HCM020000',16, null,17) stt, ten_pb,
      sum(dt_brcd_chitl) dt_brcd_chitl,
      sum(dt_mytv_chitl) dt_mytv_chitl,
      sum(dt_vnpts_chitl) dt_vnpts_chitl,
      sum(dt_vnptt_chitl) dt_vnptt_chitl,
      sum(tong_dt_chitl) tong_dt_chitl,

      sum(dt_brcd_chuachitl) dt_brcd_chuachitl,
      sum(dt_mytv_chuachitl) dt_mytv_chuachitl,
      sum(dt_vnpts_chuachitl) dt_vnpts_chuachitl,
      sum(dt_vnptt_chuachitl) dt_vnptt_chuachitl,
      sum(tong_dt_chuachitl) tong_dt_chuachitl,
      
      sum(tl_brcd) tl_brcd,
      sum(tl_mytv) tl_mytv,
      sum(tl_vnpts) tl_vnpts,
      sum(tl_vnptt) tl_vnptt,
      sum(tong_tl) tong_tl
from bangluong_dongia_202312_vttp
where tong_tl is not null or tong_dt_chuachitl is not null
group by ma_pb, ten_pb

union all
select null, 'Tong cong',
      sum(dt_brcd_chitl) dt_brcd_chitl,
      sum(dt_mytv_chitl) dt_mytv_chitl,
      sum(dt_vnpts_chitl) dt_vnpts_chitl,
      sum(dt_vnptt_chitl) dt_vnptt_chitl,
      sum(tong_dt_chitl) tong_dt_chitl,

      sum(dt_brcd_chuachitl) dt_brcd_chuachitl,
      sum(dt_mytv_chuachitl) dt_mytv_chuachitl,
      sum(dt_vnpts_chuachitl) dt_vnpts_chuachitl,
      sum(dt_vnptt_chuachitl) dt_vnptt_chuachitl,
      sum(tong_dt_chuachitl) tong_dt_chuachitl,
      
      sum(tl_brcd) tl_brcd,
      sum(tl_mytv) tl_mytv,
      sum(tl_vnpts) tl_vnpts,
      sum(tl_vnptt) tl_vnptt,
      sum(tong_tl) tong_tl
from bangluong_dongia_202312_vttp
where tong_tl is not null or tong_dt_chuachitl is not null
) order by 1;


-- Phu luc 1 - Tong hop sltb PTM theo TTVT:
select * from (
select decode(MA_PB, 'HCM140000',1,'HCM150000',2,'HCM160000',3,'HCM170000',4,'HCM180000',5,'HCM190000',6,'HCM200000',7
                        ,'HCM210000',8,'HCM220000',9,'HCM030000',10,'HCM050000',11,'HCM080000',12,'HCM110000',13,'HCM100000',14
                        ,'HCM090000',15, 'HCM020000',16, null,17) stt, ten_pb,
      sum(sl_brcd_chitl) sl_brcd_chitl,
      sum(sl_mytv_chitl) sl_mytv_chitl,
      sum(sl_vnpts_chitl) sl_vnpts_chitl,
      sum(sl_vnptt_chitl) sl_vnptt_chitl,
      sum(tong_sl_chitl) tong_sl_chitl,

      sum(sl_brcd_chuachitl) sl_brcd_chuachitl,
      sum(sl_mytv_chuachitl) sl_mytv_chuachitl,
      sum(sl_vnpts_chuachitl) sl_vnpts_chuachitl,
      sum(sl_vnptt_chuachitl) sl_vnptt_chuachitl,
      sum(tong_sl_chuachitl) tong_sl_chuachitl
      
from bangluong_dongia_202312_vttp
group by ma_pb, ten_pb

union all
select null, 'Tong cong',   
      sum(sl_brcd_chitl) sl_brcd_chitl,
      sum(sl_mytv_chitl) sl_mytv_chitl,
      sum(sl_vnpts_chitl) sl_vnpts_chitl,
      sum(sl_vnptt_chitl) sl_vnptt_chitl,
      sum(tong_sl_chitl) tong_sl_chitl,

      sum(sl_brcd_chuachitl) sl_brcd_chuachitl,
      sum(sl_mytv_chuachitl) sl_mytv_chuachitl,
      sum(sl_vnpts_chuachitl) sl_vnpts_chuachitl,
      sum(sl_vnptt_chuachitl) sl_vnptt_chuachitl,
      sum(tong_sl_chuachitl) tong_sl_chuachitl
      
from bangluong_dongia_202312_vttp
) order by stt ;



-- Phu luc 2 - Tong hop thu lao theo NVVT PTM:
select ma_nv, ten_nv, ten_to, ten_pb,
        dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl, tong_dt_chitl, 
        dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl, tong_dt_chuachitl, 
        tl_brcd, tl_mytv, tl_vnpts, tl_vnptt, tong_tl, 
        sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, tong_sl_chitl,
        sl_brcd_chuachitl, sl_mytv_chuachitl, sl_vnpts_chuachitl, sl_vnptt_chuachitl, tong_sl_chuachitl
from bangluong_dongia_202312_vttp
order by 8,6,2;


-- File excel chi tiet thue bao:
select thang_ptm, ma_gd, ma_tb, dich_vu, ten_tb, diachi_ld, ngay_bbbg, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du, 
         decode(thoihan_id,1,'ngan han',2,'dai han',null) kieuhopdong, 
         (case when thang_tldg_dnhm='202312' then doanhthu_dongia_dnhm else null end) dt_dongia_dnhm, 
         (case when thang_tldg_dnhm='202312' then luong_dongia_dnhm_nvptm else null end) tl_dnhm, thang_tldg_dnhm thang_chi_dnhm,
         doanhthu_dongia_nvptm dt_dongia_goi,
         luong_dongia_nvptm tl_goi,  
         thang_tldg_dt thang_chi,
         lydo_khongtinh_dongia lydo_chuachi,
         manv_ptm, b.ten_nv, b.ten_to, b.ten_pb
-- select * 
from ct_bsc_ptm_202312_vttp a, (select * from nhanvien_vttp where thang=202312) b
where a.manv_ptm=b.ma_nv(+) and loaitb_id<>21

union all
select thang_ptm, ma_gd, ma_tb, dich_vu, ten_tb, diachi_ld, ngay_bbbg, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du, 
         decode(thoihan_id,1,'ngan han',2,'dai han',null) kieuhopdong, 
         (case when thang_tldg_dnhm='202312' then doanhthu_dongia_dnhm else null end) dt_dongia_dnhm, 
         (case when thang_tldg_dnhm='202312' then luong_dongia_dnhm_nvptm else null end) tl_dnhm, thang_tldg_dnhm thang_chi_dnhm,
         dthu_goi_goc dt_dongia_goi,
         luong_dongia_nvptm  tl_goi,  
         thang_tldg_dt thang_chi,
         lydo_khongtinh_dongia lydo_chuachi,
         a.manv_ptm, b.ten_nv, b.ten_to, b.ten_pb
from ct_bsc_ptm_202312_vttp a, (select * from nhanvien_vttp where thang=202312) b
where a.manv_ptm=b.ma_nv(+) and a.loaitb_id=21;


-- THU HOI + PHAT HUY NVPTM: VB 137+VB 461 11/2020
update bangluong_dongia_202312_vttp a
                set   dtptm_dongia_cdbr_thuhoi='', luong_dongia_cdbr_thuhoi='',
                        dtptm_dongia_vnpts_thuhoi='', luong_dongia_vnpts_thuhoi='',
                        dtptm_dongia_vnptt_thuhoi='', luong_dongia_vnptt_thuhoi='',
                        dtptm_dongia_khac_thuhoi='', luong_dongia_khac_thuhoi='';

update bangluong_dongia_202312_vttp a  
   set (dtptm_dongia_cdbr_thuhoi, dtptm_dongia_vnpts_thuhoi, dtptm_dongia_vnptt_thuhoi, dtptm_dongia_khac_thuhoi)=
        (select dthu_ptm_cdbr, dthu_ptm_vnpts, dthu_ptm_vnptt, dthu_ptm_khac
           from tl_doanhthu_ptm
          where thang='202312' and loai_tinh='dongia_thuhoi' and ma_nv=a.ma_nv_hrm)
    -- select dtptm_dongia_cdbr_thuhoi, dtptm_dongia_vnpts_thuhoi, dtptm_dongia_vnptt_thuhoi, dtptm_dongia_khac_thuhoi from bangluong_dongia_202312_vttp a
 where exists (select * from tl_doanhthu_ptm
                        where thang='202312' and loai_tinh='dongia_thuhoi' and ma_nv=a.ma_nv_hrm);

                              
update bangluong_dongia_202312_vttp a
    set luong_dongia_cdbr_thuhoi= round(dtptm_dongia_cdbr_thuhoi*0.858 ,0)
 where dtptm_dongia_cdbr_thuhoi is not null;

update bangluong_dongia_202312_vttp a
    set luong_dongia_vnpts_thuhoi=round(dtptm_dongia_vnpts_thuhoi*0.858 ,0)
 where dtptm_dongia_vnpts_thuhoi is not null;
 
update bangluong_dongia_202312_vttp a
    set luong_dongia_vnptt_thuhoi=round(dtptm_dongia_vnptt_thuhoi*0.858  ,0)
 where dtptm_dongia_vnptt_thuhoi is not null;

update bangluong_dongia_202312_vttp a
     set luong_dongia_khac_thuhoi=round(dtptm_dongia_khac_thuhoi*0.858  ,0)
 where dtptm_dongia_khac_thuhoi is not null;
 
commit; 

 
-- tong_luong_thuhoi:
-- select sum(tong_luong_thuhoi) from bangluong_dongia_202312;  337071317
update bangluong_dongia_202312_vttp a set tong_luong_thuhoi='' where tong_luong_thuhoi is not null;
update bangluong_dongia_202312_vttp a
   set tong_luong_thuhoi=round(nvl(luong_dongia_cdbr_thuhoi,0) + nvl(luong_dongia_vnpts_thuhoi,0)
                                                            + nvl(luong_dongia_vnptt_thuhoi,0) +  nvl(luong_dongia_khac_thuhoi,0)
                                                            + nvl(giamtru_hosotainha,0) +  nvl(giamtru_phathuy_qldb,0)  ,0);
                                                            
update bangluong_dongia_202312_vttp a
   set tong_luong_thuhoi='' where tong_luong_thuhoi=0;
   
commit;


-- Kiem tra thu hoi:
select '6 field tien thu hoi' field, round( sum (nvl(luong_dongia_cdbr_thuhoi,0)  + nvl(luong_dongia_vnpts_thuhoi,0)
            +  nvl(luong_dongia_vnptt_thuhoi,0) +  nvl(luong_dongia_khac_thuhoi,0)
            +  nvl(giamtru_hosotainha,0)   +  nvl(giamtru_phathuy_qldb,0) 
             ) ,0)      tien_thuhoi
from bangluong_dongia_202312
union all
select 'tien_luong_thuhoi', sum(tong_luong_thuhoi) from bangluong_dongia_202312
union all
select '4 field tien thu hoi' field, round ( sum(nvl(luong_dongia_cdbr_thuhoi,0)  + nvl(luong_dongia_vnpts_thuhoi,0)
            +  nvl(luong_dongia_vnptt_thuhoi,0) +  nvl(luong_dongia_khac_thuhoi,0))   ,0 ) tien_thuhoi
from bangluong_dongia_202312
union all
select 'ct_thuhoi', round(sum(dthu_thu)*0.858,0) luong_thuhoi
    from ct_thuhoi a
    where thang='202312' and exists (select 1 from bangluong_dongia_202312 where ma_nv=a.ma_nv) 
union all
select 'tl_doanhthu_ptm', round(sum(tong_dthu_ptm)*0.858 ,0)
    from tl_doanhthu_ptm a
    where thang='202312'
                and exists (select 1 from bangluong_dongia_202312 where ma_nv=a.ma_nv);


/*
-- XUAT GUI BANG LUONG DON GIA = DS NHAN VIEN KO CO TRONG BANG LUONG DON GIA:
select sum(tong_luong_dongia) from bangluong_dongia_202312; 


