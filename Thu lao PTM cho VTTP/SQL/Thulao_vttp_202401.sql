-- Kiem tra
select distinct lydo_khongtinh_dongia 
from ttkd_bsc.ct_bsc_ptm
where nhom_tiepthi = 2 and thang_ptm>=202310  
                and ungdung_id = 17 and thang_tldg_dt is null;
    
    
-- Chi tiet trong ky:
	---chi tinh 5 loai hinh: VNPtt, VNPts, Fiber, MyTV, Mega
	---thang_tldg_dt : thang duoc tinh luong don gia (null: chua tinh, 999999 khong dc tinh, yyyymm thang dc tinh)
	---thang_ptm: thang insert table
	---nhom_tiepthi: tinh nhan vien VTTP (danh muc ttkd_bsc.dm_nhomld key nhomld_id)
	---thoi gian tinh trong vong 4 thang tinh tu thang_ptm, rieng VNPtt tinh 1 thang
	---VTTP chi tinh PTM, TTKD tinh PTM, nang toc do, tai lap
drop table ttkd_bsc.ct_bsc_ptm_202405_vttp;

select * from ttkd_bsc.ct_bsc_ptm_202405_vttp;
select distinct TENKIEU_LD from ttkd_bsc.ct_bsc_ptm_202405_vttp;
            
		  create table ttkd_bsc.ct_bsc_ptm_202405_vttp as
                select * from ttkd_bsc.ct_bsc_ptm
                where 
							(manv_ptm in (select ma_nv from ttkd_bsc.nhanvien where thang = 202405 and donvi = 'VTTP')
								or manv_hotro in (select ma_nv from ttkd_bsc.nhanvien where thang = 202405 and donvi = 'VTTP')
							)
                                and ((thang_ptm>=202402     ---thang n-3
										  and ((loaihd_id=1 and loaitb_id in (11,58,61)) or loaitb_id=20)
										  and (thang_tldg_dt = '202405' or thang_tldg_dt is null))          ---thang n
                                            or (loaitb_id=21 and thang_ptm = '202405'))                       ---thang n
    ;
    

                    
-- Tao bangluong_dongia_202402_vttp
drop table ttkd_bsc.bangluong_dongia_202405_vttp;
create table ttkd_bsc.bangluong_dongia_202405_vttp (
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
                    
                    tl_thuhoi_brcd number,
                    tl_thuhoi_mytv number,
                    tl_thuhoi_vnpts number,
                    tl_thuhoi_vnptt number,
                    tong_tl_thuhoi number,
                    
                    sl_thuhoi_brcd number,
                    sl_thuhoi_mytv number,
                    sl_thuhoi_vnpts number,
                    sl_thuhoi_vnptt number,
                    tong_sl_thuhoi number,
                    
                    nv_thang number 
                    );
create index ttkd_bsc.bangluong_dongia_202405_vttp_manv on ttkd_bsc.bangluong_dongia_202405_vttp (ma_nv);

select * from ttkd_bsc.nhanvien where donvi = 'VTTP' and thang = 202405;
select * from ttkd_bsc.nhanvien_vttp where thang = 202405;
select * from  ttkd_bsc.bangluong_dongia_vttp where nv_thang = 202405;

insert into ttkd_bsc.bangluong_dongia_vttp
      (ma_nv,ten_nv,ma_vtcv,ten_vtcv,ma_pb,ten_pb,ma_to,ten_to,nv_thang )
		SELECT ma_nv,ten_nv,ma_vtcv,ten_vtcv,ma_pb,ten_pb,ma_to,ten_to, thang           --thang n
		  FROM ttkd_bsc.nhanvien a
		  where donvi = 'VTTP' and thang = 202405
--					exists(select 1 from ttkd_bsc.ct_bsc_ptm_202405_vttp where manv_ptm=a.ma_nv)
--						and thang=(select max(thang) from ttkd_bsc.nhanvien_vttp where ma_nv=a.ma_nv)
	   ;
        
        select * from ttkd_bsc.bangluong_dongia_vttp;
	   select * from ttkd_bsc.th_tlptm_202405_vttp;
		commit;
    
-- tong hop:  
		drop table ttkd_bsc.x_th_tlptm_vttp
		;
					create table ttkd_bsc.x_th_tlptm_vttp as
					select manv_ptm ma_nv, 
							  (select ten_pb from ttkd_bsc.nhanvien where donvi = 'VTTP' and thang = 202405 and ma_nv = a.manv_ptm) ten_donvi,            
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
					
					    from ttkd_bsc.ct_bsc_ptm_202405_vttp
					    where thang_tldg_dt='202405' and loaitb_id != 21          ---thang n---
										and manv_ptm is not null
					    group by manv_ptm 
					    
					    union all
								 select  manv_hotro, 
									  count(case when loaitb_id in (11,58) then 1 end) sl_brcd_chitl,
									  count(case when loaitb_id=61 then 1 end) sl_mytv_chitl,
									  count(case when loaitb_id=20  then 1 end) sl_vnpts_chitl,
									  0 as sl_vnptt_chitl,
									  
									  sum(case when loaitb_id in (11,58) then doanhthu_dongia_nvhotro end) dt_brcd_chitl,
									  sum(case when loaitb_id=61 then doanhthu_dongia_nvhotro end) dt_mytv_chitl,
									  sum(case when loaitb_id=20  then doanhthu_dongia_nvhotro end) dt_vnpts_chitl,
									  0 as dt_vnptt_chitl,
							
									  sum(case when loaitb_id in (11,58) then luong_dongia_nvhotro end) tl_brcd,
									  sum(case when loaitb_id=61 then luong_dongia_nvhotro end) tl_mytv,
									  sum(case when loaitb_id=20  then luong_dongia_nvhotro end) tl_vnpts,
									  0 as tl_vnptt
							
							    from ttkd_bsc.ct_bsc_ptm_202405_vttp
							    where thang_tldg_dt_nvhotro = '202405' and loaitb_id != 21          ---thang n---
												and manv_hotro is not null
							    group by manv_hotro 
					   
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
					    from ttkd_bsc.ct_bsc_ptm_202405_vttp
					    where loaitb_id = 21 and thang_ptm = 202405 and thang_tldg_dt = '202405'              --thang n
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
					    from ttkd_bsc.ct_bsc_ptm_202405_vttp
					    where thang_tldg_dnhm = '202405'      ----thang n
									and manv_ptm is not null
					    group by manv_ptm 
					) a
					where manv_ptm is not null
					group by manv_ptm 
;


-- sltb + dthu goi chua duoc tinh
		drop table ttkd_bsc.x_th_tlptm_vttp_chuachitl
;
					create table ttkd_bsc.x_th_tlptm_vttp_chuachitl as
					select manv_ptm ma_nv, 
							  (select ten_pb from ttkd_bsc.nhanvien_vttp where thang = 202405 and ma_nv=a.manv_ptm) ten_donvi,
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
							  
					    from ttkd_bsc.ct_bsc_ptm_202405_vttp
					    where loaitb_id in (11,58,61,20) and thang_tldg_dt is null  and manv_ptm is not null
					    group by manv_ptm 
					    
					    union all
								select  manv_hotro, 
										  count(case when loaitb_id in (11,58) then 1 end) sl_brcd_chuachitl,
										  count(case when loaitb_id=61 then 1 end) sl_mytv_chuachitl,
										  count(case when loaitb_id=20  then 1 end) sl_vnpts_chuachitl,
										  0 as  sl_vnptt_chuachitl,
										  
										  sum(case when loaitb_id in (11,58) then doanhthu_dongia_nvhotro end) dt_brcd_chuachitl,
										  sum(case when loaitb_id=61 then doanhthu_dongia_nvhotro end) dt_mytv_chuachitl,
										  sum(case when loaitb_id=20  then doanhthu_dongia_nvhotro end) dt_vnpts_chuachitl,
										  0 as  dt_vnptt_chuachitl
										  
								    from ttkd_bsc.ct_bsc_ptm_202405_vttp
								    where loaitb_id in (11,58,61,20) and thang_tldg_dt_nvhotro is null and manv_hotro is not null
								    group by manv_hotro 
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
					    from ttkd_bsc.ct_bsc_ptm_202405_vttp
					    where loaitb_id=21 and thang_tldg_dnhm is null -- and nvl(doanhthu_kpi_nvhotro,0)>0
					    group by manv_ptm
					    ) a
					group by manv_ptm
		;
    
 
-- ptm cdbr + gtgt +vnpts             
			update ttkd_bsc.bangluong_dongia_vttp a
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
					  tong_dt_chuachitl=''
				where thang = 202405
	;
    
            
update ttkd_bsc.bangluong_dongia_vttp a
            set ( sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, 
                    dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl,
                    tl_brcd, tl_mytv, tl_vnpts, tl_vnptt)
           =
		 (select sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, 
                    dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl,
                    tl_brcd, tl_mytv, tl_vnpts, tl_vnptt
             from ttkd_bsc.x_th_tlptm_vttp
             where ma_nv=a.ma_nv)
    -- select * from ttkd_bsc.bangluong_dongia_vttp a
    where thang = 202405
				and exists(select 1 from ttkd_bsc.x_th_tlptm_vttp where ma_nv=a.ma_nv)
    
    
    ;


update ttkd_bsc.bangluong_dongia_vttp a
            set (sl_brcd_chuachitl, sl_mytv_chuachitl, sl_vnpts_chuachitl, sl_vnptt_chuachitl,
                    dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl)
            =(select sl_brcd_chuachitl, sl_mytv_chuachitl, sl_vnpts_chuachitl, sl_vnptt_chuachitl,
                    dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl
              from ttkd_bsc.x_th_tlptm_vttp_chuachitl
              where ma_nv=a.ma_nv)
    -- select * from ttkd_bsc.bangluong_dongia_vttp a
    where thang = 202405
				and exists(select 1 from ttkd_bsc.x_th_tlptm_vttp_chuachitl where ma_nv=a.ma_nv)
    
    ;
    
    

update ttkd_bsc.bangluong_dongia_vttp set tong_dt_chitl='', tong_tl='' , tong_dt_chuachitl='',  tong_sl_chitl='', tong_sl_chuachitl='' where thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp a
            set  tong_dt_chitl      = nvl(dt_brcd_chitl,0)+nvl(dt_mytv_chitl,0)+nvl(dt_vnpts_chitl,0)+nvl(dt_vnptt_chitl,0)    
                    ,tong_tl                = nvl(tl_brcd,0)+nvl(tl_mytv,0)+nvl(tl_vnpts,0)+nvl(tl_vnptt,0)
                    ,tong_dt_chuachitl = nvl(dt_brcd_chuachitl,0)+nvl(dt_mytv_chuachitl,0)+nvl(dt_vnpts_chuachitl,0)+nvl(dt_vnptt_chuachitl,0)
                    ,tong_sl_chitl      = nvl(sl_brcd_chitl,0)+nvl(sl_mytv_chitl,0)+nvl(sl_vnpts_chitl,0)+nvl(sl_vnptt_chitl,0)   
                    ,tong_sl_chuachitl = nvl(sl_brcd_chuachitl,0)+nvl(sl_mytv_chuachitl,0)+nvl(sl_vnpts_chuachitl,0)+nvl(sl_vnptt_chuachitl,0)
	where thang = 202405
				;


update ttkd_bsc.bangluong_dongia_vttp set dt_brcd_chitl='' where dt_brcd_chitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set dt_mytv_chitl='' where dt_mytv_chitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set dt_vnpts_chitl='' where dt_vnpts_chitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set dt_vnptt_chitl='' where dt_vnptt_chitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set tong_dt_chitl='' where tong_dt_chitl=0 and thang = 202405;

update ttkd_bsc.bangluong_dongia_vttp set tl_brcd='' where tl_brcd=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set tl_mytv='' where tl_mytv=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set tl_vnpts='' where tl_vnpts=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set tl_vnptt='' where tl_vnptt=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set tong_tl='' where tong_tl=0 and thang = 202405;
            
update ttkd_bsc.bangluong_dongia_vttp set dt_brcd_chuachitl='' where dt_brcd_chuachitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set dt_mytv_chuachitl='' where dt_mytv_chuachitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set dt_vnpts_chuachitl='' where dt_vnpts_chuachitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set dt_vnptt_chuachitl='' where dt_vnptt_chuachitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set tong_dt_chuachitl='' where tong_dt_chuachitl=0 and thang = 202405;
            
update ttkd_bsc.bangluong_dongia_vttp set sl_brcd_chitl='' where sl_brcd_chitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set sl_mytv_chitl='' where sl_mytv_chitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set sl_vnpts_chitl='' where sl_vnpts_chitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set sl_vnptt_chitl='' where sl_vnptt_chitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set tong_sl_chitl='' where tong_sl_chitl=0 and thang = 202405;

update ttkd_bsc.bangluong_dongia_vttp set sl_brcd_chuachitl='' where sl_brcd_chuachitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set sl_mytv_chuachitl='' where sl_mytv_chuachitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set sl_vnpts_chuachitl='' where sl_vnpts_chuachitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set sl_vnptt_chuachitl='' where sl_vnptt_chuachitl=0 and thang = 202405;
update ttkd_bsc.bangluong_dongia_vttp set tong_sl_chuachitl='' where tong_sl_chuachitl=0 and thang = 202405;

            commit;
-- Thu hoi:
				update ttkd_bsc.bangluong_dongia_vttp a  
				   set tl_thuhoi_brcd = '', tl_thuhoi_mytv = '', tl_thuhoi_vnpts = '', tl_thuhoi_vnptt = '', tong_tl_thuhoi = ''
						, sl_thuhoi_brcd='', sl_thuhoi_mytv='', sl_thuhoi_vnpts='', sl_thuhoi_vnptt='', tong_sl_thuhoi=''
				   where tong_tl_thuhoi is not null
								and thang = 202405
				   ;
--   select * from ttkd_bsc.bangluong_dongia_202405_vttp a
				update ttkd_bsc.bangluong_dongia_vttp a  
				   set (tl_thuhoi_brcd, tl_thuhoi_mytv, tl_thuhoi_vnpts, tl_thuhoi_vnptt, tong_tl_thuhoi, sl_thuhoi_brcd, sl_thuhoi_mytv, sl_thuhoi_vnpts, sl_thuhoi_vnptt, tong_sl_thuhoi) 
							 = (select sum(case when loaitb_id=58 then tien_thuhoi end)
										  , sum(case when loaitb_id=61 then tien_thuhoi end)
										  , sum(case when loaitb_id=20 then tien_thuhoi end)
										  , sum(case when loaitb_id=21 then tien_thuhoi end)
										  , sum(case when loaitb_id in (58, 61, 20, 21) then tien_thuhoi end)
										  , sum(case when loaitb_id=58 then 1 end)
										  , sum(case when loaitb_id=61 then 1 end)
										  , sum(case when loaitb_id=20 then 1 end)
										  , sum(case when loaitb_id=21 then 1 end)
										  ,count(case when loaitb_id in (58, 61, 20, 21) then 1 end)
								  from ttkd_bsc.ct_thuhoi  
								  where thang = 202405 and ma_nv =a.ma_nv)             ---thang n
				   where exists(select 1 from ttkd_bsc.ct_thuhoi 
									   where thang = 202405 and ma_nv = a.ma_nv)                         ---thang n
				;
				select * from ttkd_bsc.ct_thuhoi  
								  where thang=202405;
				
			delete from ttkd_bsc.bangluong_dongia_202405_vttp
			    where tong_tl is null and tong_dt_chuachitl is null and tong_sl_chitl is null and tong_sl_chuachitl is null and tong_sl_thuhoi is null;
    
commit;

select * from ttkd_bsc.bangluong_dongia_202404_vttp_l1;
select * from ttkd_bsc.ct_thuhoi where thang=202404 and ma_pb = 'HCM160000';
select * from ttkd_bsc.ct_bsc_ptm where id = 5571575;

-- BBXN-DTPTM-THULAO - Tong hop doanh thu/thu lao PTM theo TTVT:
select rownum stt, ten_pb, dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl, tong_dt_chitl
			, dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl, tong_dt_chuachitl
			, tl_brcd, tl_mytv, tl_vnpts, tl_vnptt, tong_tl
		  , tl_thuhoi_brcd, tl_thuhoi_mytv, tl_thuhoi_vnpts, tl_thuhoi_vnptt, tong_tl_thuhoi
from (
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
					 sum(tong_tl) tong_tl,
					 
					 sum(tl_thuhoi_brcd) tl_thuhoi_brcd,
					 sum(tl_thuhoi_mytv) tl_thuhoi_mytv,
					 sum(tl_thuhoi_vnpts) tl_thuhoi_vnpts,
					 sum(tl_thuhoi_vnptt) tl_thuhoi_vnptt,
					 sum(tong_tl_thuhoi) tong_tl_thuhoi
				from ttkd_bsc.bangluong_dongia_vttp
				where thang = 202405
								and (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null)
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
					 sum(tong_tl) tong_tl,
					 
					 sum(tl_thuhoi_brcd) tl_thuhoi_brcd,
					 sum(tl_thuhoi_mytv) tl_thuhoi_mytv,
					 sum(tl_thuhoi_vnpts) tl_thuhoi_vnpts,
					 sum(tl_thuhoi_vnptt) tl_thuhoi_vnptt,
					 sum(tong_tl_thuhoi) tong_tl_thuhoi
				from ttkd_bsc.bangluong_dongia_vttp
				where thang = 202405
								and (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null)
				order by 1
) a 
;

select tl_thuhoi_brcd, tl_thuhoi_mytv, tl_thuhoi_vnpts, tl_thuhoi_vnptt, tong_tl_thuhoi
--			sum(tl_thuhoi_brcd) tl_thuhoi_brcd,
--			 sum(tl_thuhoi_mytv) tl_thuhoi_mytv,
--			 sum(tl_thuhoi_vnpts) tl_thuhoi_vnpts,
--			 sum(tl_thuhoi_vnptt) tl_thuhoi_vnptt,
--			 sum(tong_tl_thuhoi) tong_tl_thuhoi
from ttkd_bsc.bangluong_dongia_202404_vttp
where (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null) and ma_pb = 'HCM160000';
order by 1
;

-- BBXN-TBPTM - Tong hop sltb PTM theo TTVT:
			select rownum stt, ten_pb, sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, tong_sl_chitl, sl_brcd_chuachitl, sl_mytv_chuachitl,
					  sl_vnpts_chuachitl, sl_vnptt_chuachitl, tong_sl_chuachitl, sl_thuhoi_brcd, sl_thuhoi_mytv, sl_thuhoi_vnpts, sl_thuhoi_vnptt, tong_sl_thuhoi
			from (
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
				 sum(tong_sl_chuachitl) tong_sl_chuachitl,
				 
				 sum(sl_thuhoi_brcd) sl_thuhoi_brcd,
				 sum(sl_thuhoi_mytv) sl_thuhoi_mytv,
				 sum(sl_thuhoi_vnpts) sl_thuhoi_vnpts,
				 sum(sl_thuhoi_vnptt) sl_thuhoi_vnptt,
				 sum(tong_sl_thuhoi) tong_sl_thuhoi
			from ttkd_bsc.bangluong_dongia_vttp
			where thang = 202405
							and (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null)
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
				 sum(tong_sl_chuachitl) tong_sl_chuachitl,
			
				 sum(sl_thuhoi_brcd) sl_thuhoi_brcd,
				 sum(sl_thuhoi_mytv) sl_thuhoi_mytv,
				 sum(sl_thuhoi_vnpts) sl_thuhoi_vnpts,
				 sum(sl_thuhoi_vnptt) sl_thuhoi_vnptt,
				 sum(tong_sl_thuhoi) tong_sl_thuhoi      
			from ttkd_bsc.bangluong_dongia_vttp
			where thang = 202405
							and (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null)
			order by stt 
			) a
			;



-- BBXN-DTPTM-THULAO_NV - Tong hop thu lao theo NVVT PTM:
		select ma_nv, ten_nv, ten_to, ten_pb,
			   dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl, tong_dt_chitl, 
			   dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl, tong_dt_chuachitl, 
			   tl_brcd, tl_mytv, tl_vnpts, tl_vnptt, tong_tl, 
			   sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, tong_sl_chitl,
			   sl_brcd_chuachitl, sl_mytv_chuachitl, sl_vnpts_chuachitl, sl_vnptt_chuachitl, tong_sl_chuachitl,
			   tong_tl_thuhoi, tong_sl_thuhoi
		from ttkd_bsc.bangluong_dongia_vttp
		where thang = 202405
							and (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null)
		order by 8,6,2
		;
		---File tonghop_chitiet__nhanvien_0x202x.xlsx
			select ma_nv, ten_nv, ten_to, ten_pb,
						 tong_tl tong_thulao,			 
						   tong_tl_thuhoi tong_thuhoi, tong_tl - tong_tl_thuhoi  Thulao_chitra
						   , substr(ma_nv, 1,3) ghichu
		from ttkd_bsc.bangluong_dongia_vttp
		where thang = 202405
						and (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null)
		order by 4,3,2
		;

-- File excel chi tiet thue bao:
	select *
	from(
				select 'PTM' nguon, thang_ptm
--						, ma_tb, dich_vu, ten_tb, diachi_ld, ngay_bbbg, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du, 
--				         decode(thoihan_id,1,'ngan han',2,'dai han',null) kieuhopdong, 
--				         (case when thang_tldg_dnhm='202404' then doanhthu_dongia_dnhm else null end) dt_dongia_dnhm,       --thang n
--				         (case when thang_tldg_dnhm='202404' then luong_dongia_dnhm_nvptm else null end) tl_dnhm, thang_tldg_dnhm thang_chi_dnhm,           --thang n
--				         doanhthu_dongia_nvptm dt_dongia_goi,
--				         luong_dongia_nvptm tl_goi,  
--				         thang_tldg_dt thang_chi,
						, ma_gd, ma_tb, dich_vu, ngay_bbbg
				         , lydo_khongtinh_dongia lydo_chuachi
					    , manv_ptm, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb
					    , (case when thang_tldg_dnhm='202405' then doanhthu_dongia_dnhm else 0 end) -- dt_dongia_dnhm
							+ (case when thang_tldg_dt='202405' then doanhthu_dongia_nvptm else 0 end) --dt_dongia_goi
							as tong_dt_dongia_goi
						, (case when thang_tldg_dnhm='202405' then luong_dongia_dnhm_nvptm else 0 end)  --tl_dnhm
							+ (case when thang_tldg_dt='202405' then luong_dongia_nvptm else 0 end) --tl_goi
							as tong_tl_goi
				-- select * 
				from ttkd_bsc.ct_bsc_ptm_202405_vttp a, (select * from ttkd_bsc.nhanvien where donvi = 'VTTP' and thang = 202405) b     --thang n
				where a.manv_ptm=b.ma_nv(+) and loaitb_id in (58, 61, 20, 11)
				
				union all
				select 'PTM' nguon, thang_ptm--, ma_gd, ma_tb, dich_vu, ten_tb, diachi_ld, ngay_bbbg, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du, 
				--         decode(thoihan_id,1,'ngan han',2,'dai han',null) kieuhopdong, 
				--         (case when thang_tldg_dnhm='202404' then doanhthu_dongia_dnhm else null end) dt_dongia_dnhm, 
				--         (case when thang_tldg_dnhm='202404' then luong_dongia_dnhm_nvptm else null end) tl_dnhm, thang_tldg_dnhm thang_chi_dnhm,
				--         dthu_goi_goc dt_dongia_goi,
				--         luong_dongia_nvptm  tl_goi,  
				--         thang_tldg_dt thang_chi,
				--         lydo_khongtinh_dongia lydo_chuachi,
						, ma_gd, ma_tb, dich_vu, ngay_bbbg
				         , lydo_khongtinh_dongia lydo_chuachi
					    , a.manv_ptm, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb
						 , (case when thang_tldg_dnhm='202405' then tien_dnhm else 0 end) -- dt_dongia_dnhm
							+ (case when thang_tldg_dt='202405' then dthu_goi_goc else 0 end) --dt_dongia_goi
							as tong_dt_dongia_goi
						, (case when thang_tldg_dnhm='202405' then luong_dongia_dnhm_nvptm else 0 end)  --tl_dnhm
							+ (case when thang_tldg_dt='202405' then luong_dongia_nvptm else 0 end) --tl_goi
							as tong_tl_goi
				from ttkd_bsc.ct_bsc_ptm_202405_vttp a, (select * from ttkd_bsc.nhanvien where donvi = 'VTTP' and thang=202405) b
				where a.manv_ptm=b.ma_nv and a.loaitb_id=21
		) where TONG_DT_DONGIA_GOI is not null  or TONG_TL_GOI is not null
		
		
		union all

-- CT_THUHOI
		select 'Thuhoi' nguon, cast(thang_ptm as number) thang
				, null, ma_tb, null, null
				 , loai_thu ||'; '|| ghichu lydo_chuachi
			 , a.ma_nv, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb
			 ,doanhthu_dongia,tien_thuhoi 
		 from ttkd_bsc.ct_thuhoi a, ttkd_bsc.nhanvien b
		 where a.thang=202405 and a.loaitb_id in (58, 61, 20, 21) 
						and a.ma_nv=b.ma_nv and a.thang = b.thang and b.donvi = 'VTTP'
		--   and exists(select * from ttkd_bsc.bangluong_dongia_202405_vttp where ma_nv=a.ma_nv)
   ;

select * from  ttkd_bsc.ct_thuhoi
;
select ma_nv, count(ma_to) from ttkd_bsc.ct_bsc_ptm_202404_vttp where thang=202404 
group by ma_nv;

/*
-- XUAT GUI BANG LUONG DON GIA = DS NHAN VIEN KO CO TRONG BANG LUONG DON GIA:
select sum(tong_luong_dongia) from bangluong_dongia_202402; 


  
    
/* ---- Kiem tra doanh thu 
select 'bangluong_dongia_202402_vttp' , sum(tl_brcd), sum(tl_mytv) , sum(tl_vnpts), sum(tl_vnptt) from bangluong_dongia_202402_vttp 
union all
select 'ct_ptm - luong goi',
            sum(case when loaitb_id=58 then luong_dongia_nvptm end) tl_brcd,
            sum(case when loaitb_id=61 then luong_dongia_nvptm end) tl_mytv,
            sum(case when loaitb_id=20 then luong_dongia_nvptm end) tl_vnpts,
            sum(case when loaitb_id=21 then luong_dongia_nvptm end) tl_vnptt       
    from ct_bsc_ptm_202402_vttp
    where thang_tldg_dt='202402'

union all
select 'ct_ptm - luong goi',
            sum(case when loaitb_id=58 then luong_dongia_dnhm_nvptm end) tl_brcd,
            sum(case when loaitb_id=61 then luong_dongia_dnhm_nvptm end) tl_mytv,
            sum(case when loaitb_id=20 then luong_dongia_dnhm_nvptm end) tl_vnpts,
            sum(case when loaitb_id=21 then luong_dongia_dnhm_nvptm end) tl_vnptt       
    from ct_bsc_ptm_202402_vttp
    where thang_tldg_dnhm='202402';


       
-- Kiem tra so luong tb
select 'bangluong_dongia_202402_vttp_chi' , sum(tong_sl_chitl) tong_sl
 from bangluong_dongia_202402_vttp 
union all
select 'bangluong_dongia_202402_vttp_chi',  sum(tong_sl_chuachitl) tong_sl
 from bangluong_dongia_202402_vttp
union all 
select 'bangluong_dongia_202402_vttp_tong' , sum(tong_sl_chitl) + sum(tong_sl_chuachitl) tong_sl
 from bangluong_dongia_202402_vttp 
union all
select 'ct_bsc_ptm_202402_vttp', count(*) tong_sl
 from ct_bsc_ptm_202402_vttp;
                
*/

---Loai nvien TTVT chuyen cong tac sang PosMacCo t? T3/2024
(select ma_nv from ttkd_bsc.nhanvien_vttp_potmasco);
ttkd_bsc.bangluong_dongia_202404_vttp_postmaco;
ttkd_bsc.ct_bsc_ptm_202404_vttp_postmaco



