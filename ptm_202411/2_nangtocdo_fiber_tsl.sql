drop table ttkd_bct.thaydoitocdo_202403 purge;
create table ttkd_bct.thaydoitocdo_202403 as
SELECT DISTINCT  
      a.ma_gd, a.ma_kh, b.ma_tb, lhtb.loaihinh_tb dich_vu, kld.ten_kieuld tenkieu_ld, b.kieuld_id, a.loaihd_id, 
      b.dichvuvt_id, b.loaitb_id loaitb_id, a.hdkh_id, b.hdtb_id, dbkh.so_gt, dbkh.mst mst_kh, dbtt.mst mst_tt, 
      a.khachhang_id, dbtb_new.thanhtoan_id, b.thuebao_id, dbtb_new.chuquan_id, dbtb_new.ten_tb, dbtb_new.diachi_ld, 
      dbtb_new.doituong_id, dbtb_new.ngay_sd, b.ngay_ht, b.ngay_kh, b.ngay_ins, dbtb_new.trangthaitb_id, a.ma_duan ma_duan_banhang,       
      a.ctv_id, a.nhanvien_id, TRIM(UPPER(dm.ma_nv)) ma_tiepthi, TRIM(UPPER(dm.ma_nv)) ma_tiepthi_new, 
      dm.ten_nv ten_tiepthi, dm1.ten_dv to_tt,dm2.ten_dv donvi_tt, 
      decode(b.tucthi,0,'Thang sau','Tuc thi') tucthi,
      dbtb_old.tocdo_id tocdo_dbold_id,  dbtb_new.tocdo_id tocdo_dbnew_id,     
      dbtb_old.mucuoctb_id mucuoctb_id_old, b.mucuoctb_id mucuoctb_id_new, 
      (CASE WHEN b.dichvuvt_id IN (7, 8, 9) THEN (SELECT TO_CHAR(tocdo) || donvi FROM css.tocdo_kenh@dataguard b WHERE b.tocdo_id = dbtb_old.tocdo_id AND dbtb_old.tocdo_id > 0) 
            WHEN b.dichvuvt_id = 4 THEN (SELECT ma_td FROM css.tocdo_adsl@dataguard b WHERE b.tocdo_id = dbtb_old.tocdo_id AND dbtb_old.tocdo_id > 0) 
          ELSE NULL END) ma_td_old, 

      (CASE WHEN b.dichvuvt_id IN (7, 8, 9) THEN (SELECT TO_CHAR(tocdo) || donvi FROM css.tocdo_kenh@dataguard b WHERE b.tocdo_id = dbtb_new.tocdo_id AND dbtb_new.tocdo_id > 0) 
            WHEN b.dichvuvt_id = 4 THEN (SELECT ma_td FROM css.tocdo_adsl@dataguard b WHERE b.tocdo_id = dbtb_new.tocdo_id AND dbtb_new.tocdo_id > 0) 
          ELSE NULL END) ma_td_new, 
            
      (CASE WHEN b.dichvuvt_id=4 THEN (SELECT cuoc_tg FROM css.v_muccuoc_tb@dataguard mc WHERE mc.dichvuvt_id=b.dichvuvt_id AND mc.mucuoctb_id=dbtb_old.mucuoctb_id)
                 WHEN b.dichvuvt_id in (7,8,9) AND km_old.tyle_sd IS NULL THEN nvl(dbtb_old.cuoc_tk_goc,0)+nvl(dbtb_old.cuoc_tc_goc,0)+nvl(dbtb_old.cuoc_nix,0)+nvl(dbtb_old.cuoc_isp,0)  
                 WHEN b.dichvuvt_id in (7,8,9) AND km_old.tyle_sd IS NOT NULL THEN round( (nvl(dbtb_old.cuoc_tk_goc,0)+nvl(dbtb_old.cuoc_tc_goc,0)+nvl(dbtb_old.cuoc_nix,0)+nvl(dbtb_old.cuoc_isp,0)) *(100- nvl(km_old.tyle_sd,0) )/100,0)
            ELSE NULL END) dthu_goi_old,
            
      (CASE WHEN b.dichvuvt_id=4 then (SELECT cuoc_tg FROM css.v_muccuoc_tb@dataguard mc WHERE mc.dichvuvt_id=b.dichvuvt_id AND mc.mucuoctb_id=dbtb_new.mucuoctb_id)
                 WHEN b.dichvuvt_id in (7,8,9) AND km_old.tyle_sd  IS NULL THEN nvl(dbtb_new.cuoc_tk_goc,0)+nvl(dbtb_new.cuoc_tc_goc,0)+nvl(dbtb_new.cuoc_nix,0)+nvl(dbtb_new.cuoc_isp,0)  
                 WHEN b.dichvuvt_id in (7,8,9) AND km_old.tyle_sd  IS NOT NULL THEN round( (nvl(dbtb_new.cuoc_tk_goc,0)+nvl(dbtb_new.cuoc_tc_goc,0)+nvl(dbtb_new.cuoc_nix,0)+nvl(dbtb_new.cuoc_isp,0)) *(100- nvl(km_new.tyle_sd,0) )/100,0)
            ELSE NULL END) dthu_goi_new         

  FROM css.v_hd_khachhang@dataguard a,
       ( -- fiber:        
         SELECT hd.*, hd_adsl.tucthi FROM css.v_hd_thuebao@dataguard hd, css.v_hdtb_adsl@dataguard hd_adsl
           WHERE 
              hd.phanvung_id = 28 AND hd_adsl.phanvung_id = 28 AND hd.hdtb_id=hd_adsl.hdtb_id 
             AND hd.loaitb_id = 58
             AND hd.tthd_id IN (5, 6)
             AND hd.nguoi_cn <> 'ccbs'
             AND hd.kieuld_id IN (24, 654, 13080)
             AND hd.ngay_kh  BETWEEN TO_DATE('202402', 'yyyyMM') AND ADD_MONTHS(TO_DATE('202403', 'yyyyMM'), 1) - 1/86400
             AND hd.ngay_kh = (SELECT MAX(ngay_kh)
                               FROM css.v_hd_thuebao@dataguard
                               WHERE phanvung_id = 28
                                 AND tthd_id IN (5, 6)
                                 AND loaitb_id = 58
                                 AND TO_CHAR(ngay_kh, 'yyyymm') BETWEEN '202402' AND '202403'
                                 AND kieuld_id IN (24, 654, 13080)
                                 AND thuebao_id = hd.thuebao_id
                            )
             AND (hd.ghichu NOT LIKE 'Tu dong doi toc do%' OR hd.ghichu IS NULL)
             

           UNION ALL
         -- TSL:  
         SELECT hd.*, null as tucthi FROM css.v_hd_thuebao@dataguard hd
           WHERE phanvung_id = 28
             AND dichvuvt_id IN (7, 8, 9)
             AND tthd_id IN (5, 6) 
             AND kieuld_id IN (64, 65, 596, 684, 696, 190, 702, 13127, 952)
             AND ngay_ht BETWEEN TO_DATE('202403', 'yyyyMM') AND ADD_MONTHS(TO_DATE('202403', 'yyyyMM'), 1) - 1/86400            
             AND ngay_ht = (SELECT MAX(ngay_ht)
                               FROM css.v_hd_thuebao@dataguard hd1
                               WHERE --phanvung_id = 28 AND
                                 TO_CHAR(hd1.ngay_ht, 'yyyymm') = '202403' 
                                 AND hd1.kieuld_id IN (64, 65, 596, 684, 696, 190, 702, 13127, 952)
                                 AND hd1.thuebao_id = hd.thuebao_id
                            )
      
   /*        UNION ALL
         -- Nang cap Colocation:
         SELECT hd.*, null as tucthi FROM css.v_hd_thuebao@dataguard hd
           WHERE phanvung_id = 28
             AND loaitb_id = 39
             AND tthd_id IN (5, 6)
             AND kieuld_id IN (785, 786, 13153, 13166, 13119)
             AND ngay_ht BETWEEN TO_DATE('202403', 'yyyyMM') AND ADD_MONTHS(TO_DATE('202403', 'yyyyMM'), 1) - 1/86400
*/
       ) b,

       admin.v_donvi@dataguard c,
       admin.v_nhanvien@dataguard nv,
       admin.v_nhanvien@dataguard dm,
       admin.v_donvi@dataguard dm1,
       admin.v_donvi@dataguard dm2,
       css.loaihinh_tb@dataguard lhtb,
       css.kieu_ld@dataguard kld,

       tinhcuoc.v_dbtb@dataguard dbtb_old,
       tinhcuoc.v_dbtb@dataguard dbtb_new,

       css.v_db_thanhtoan@dataguard dbtt,
       css.v_db_khachhang@dataguard dbkh,

       (SELECT thuebao_id, tyle_sd FROM css.v_khuyenmai_dbtb@dataguard
           WHERE phanvung_id = 28 AND hieuluc = 0
             AND tyle_sd > 0
             AND tyle_sd < 100
             AND thang_kt = 202402
        UNION ALL
        SELECT thuebao_id, tyle_tk FROM css.v_tb_khuyenmai@dataguard
           WHERE phanvung_id = 28 AND hieuluc = 0
             AND tyle_sd > 0
             AND tyle_sd < 100
             AND thang_huy = 202402
        ) km_old,
       (SELECT * FROM css.v_khuyenmai_dbtb@dataguard
           WHERE phanvung_id = 28 AND hieuluc = 1
             AND tyle_sd > 0
             AND tyle_sd < 100
             AND thang_kt >= 202403) km_new       

  WHERE dbtb_old.ky_cuoc (+) = 20240201
    AND dbtb_new.ky_cuoc (+) = 20240301
    AND dbtb_old.phanvung_id (+) = 28
    AND dbtb_new.phanvung_id (+) = 28
    AND a.phanvung_id = 28
    AND b.phanvung_id = 28
    AND dbtt.phanvung_id (+) = 28
    AND dbkh.phanvung_id (+) = 28

    AND a.hdkh_id = b.hdkh_id
    AND b.thuebao_id = dbtb_old.thuebao_id (+)
    AND b.thuebao_id = dbtb_new.thuebao_id (+)

    AND dbtb_new.thanhtoan_id = dbtt.thanhtoan_id (+)
    AND dbtb_new.khachhang_id = dbkh.khachhang_id (+)

    AND a.donvi_id = c.donvi_id (+)
    AND a.nhanvien_id = nv.nhanvien_id (+)
    AND a.ctv_id = dm.nhanvien_id (+)
    AND dm.donvi_id = dm1.donvi_id (+)
    AND dm1.donvi_cha_id = dm2.donvi_id (+)
    
    AND dbtb_new.loaitb_id = lhtb.loaitb_id 
    AND b.kieuld_id = kld.kieuld_id

   -- khuyen mai tsl:
    AND b.thuebao_id = km_old.thuebao_id (+)
    AND b.thuebao_id = km_new.thuebao_id (+)
    AND dbtb_new.doituong_id (+) NOT IN (14, 15, 16, 17, 19, 32, 33, 34, 35, 36, 48, 51, 52, 54, 55, 56, 57, 66, 47, 49, 50) -- may nghiep vu, cong vu, nghiep vu X, VMS,STTTT, BOCA
;


create table thaydoitocdo_202403_goc as select * from thaydoitocdo_202403;
create index thaydoitocdo_202403_hdtbid on thaydoitocdo_202403 (hdtb_id);
create index thaydoitocdo_202403_thuebaoid on thaydoitocdo_202403 (thuebao_id);
create index thaydoitocdo_202403_maduan on thaydoitocdo_202403 (ma_duan_banhang);


alter table thaydoitocdo_202403 
    add ( tien_dvgt_bs number, 
              tien_tbi_bs number,
              dthu_ps_old number,
              dthu_ps_new number,
              dthu_duoctinh number,
              lydo_khongtinh_luong varchar2(200),
              ghi_chu varchar2(500),
              ngay_luuhs_ttkd date, 
              ngay_luuhs_ttvt date, 
              nop_du number);

select * from thaydoitocdo_202403_goc;
--create table thaydoitocdo_202403_goc as select * from thaydoitocdo_202403;    -- luu bang truoc khi delete
delete from thaydoitocdo_202403 a
    -- select * from thaydoitocdo_202403 a
    where loaitb_id in (58,59) 
                and ( (tucthi='Tuc thi' and to_char(ngay_kh,'yyyymm')='202402') 
                            or (tucthi='Thang sau' and to_char(ngay_kh,'yyyymm')='202403')
                            or exists(select 1 from ttkd_bsc.ct_bsc_ptm where hdtb_id=a.hdtb_id));
            
-- ma_duan:
drop table temp_tbduan;
create table temp_tbduan as 
    select thuebao_id, ma_duan from ttkd_bsc.v_db a
        where exists(select 1 from thaydoitocdo_202403 where thuebao_id=a.thuebao_id)
            and ma_duan is not null;
create index temp_tbduan_1 on temp_tbduan (thuebao_id);

alter table thaydoitocdo_202403 add ma_duan varchar2(20);
update thaydoitocdo_202403 a 
        set ma_duan=(select ma_duan from temp_tbduan where thuebao_id=a.thuebao_id);
     

-- dthu_ps_old,dthu_ps_new :
update thaydoitocdo_202403 a set dthu_ps_old='', dthu_ps_new='';
update thaydoitocdo_202403 a 
        set dthu_ps_old=(select sum(dthu) from ttkd_bct.cuoc_thuebao_ttkd_202402 where thuebao_id=a.thuebao_id),
              dthu_ps_new=(select sum(nogoc) from bcss_hcm.ct_no
                                         where ky_cuoc=20240301 and khoanmuctt_id not in (441,520,521,527,3126,3127,3421,3953) 
                                                and thuebao_id=a.thuebao_id);


--  Bo sung dthu_goi_old, dthu_goi_new:
update thaydoitocdo_202403 a 
    set dthu_goi_old = (select distinct cuoc_tggoc from bcss_hcm.thftth where ky_cuoc=20240201 and ma_tb=a.ma_tb)
   -- select * from thaydoitocdo_202403 a
   where loaitb_id in (58) and chuquan_id in (145,264,266) and nvl(dthu_goi_old,0)=0;
 
update thaydoitocdo_202403 a 
    set dthu_goi_new = (select cuoc_tggoc from bcss.v_thftth@dataguard where ky_cuoc=20240301 and ma_tb=a.ma_tb)
    -- select * from thaydoitocdo_202403 a
    where loaitb_id in (58) and nvl(dthu_goi_new,0)=0;
    
-- Bo sung iptinh
alter table thaydoitocdo_202403 add iptinh number;
update thaydoitocdo_202403 a 
    set iptinh = (select cuoc_ipgoc from bcss.v_thftth@dataguard where ky_cuoc=20240301 and ma_tb=a.ma_tb)
   -- select * from thaydoitocdo_202403 a
   where loaitb_id in (58) and chuquan_id in (145,264,266) 
        and exists (select 1 from bcss_hcm.thftth where ky_cuoc=20240201 and cuoc_ipgoc=0 and ma_tb=a.ma_tb)
        and exists (select 1 from bcss_hcm.thftth where ky_cuoc=20240301 and cuoc_ipgoc>0 and ma_tb=a.ma_tb);

            
update thaydoitocdo_202403 a 
    set dthu_goi_new = nvl(dthu_goi_new,0) + iptinh
    where iptinh>0;


-- dthu_duoctinh:
update thaydoitocdo_202403 set dthu_duoctinh='';
update thaydoitocdo_202403 
    set dthu_duoctinh=(case when nvl(dthu_goi_new,0)-nvl(dthu_goi_old,0) > 0  
                                                then nvl(dthu_goi_new,0)-nvl(dthu_goi_old,0) else 0 end);
                                                   

-- lydo_khongtinh_luong:
update ttkd_bct.thaydoitocdo_202404 a set lydo_khongtinh_luong=''
			--where (THUEBAO_ID, HDTB_ID) IN (SELECT  THUEBAO_ID, HDTB_ID FROM hocnq_ttkd.ptm_xuly_khieunai_tocdo_202404)
			;
	update ttkd_bct.thaydoitocdo_202404 a 
	    set lydo_khongtinh_luong=lydo_khongtinh_luong||'-Tinh luong theo hs lap moi'
	    -- select lydo_khongtinh_luong from ttkd_bct.thaydoitocdo_202404 a
	    where (exists (select 1 from ttkd_bsc.ct_bsc_ptm
							  where thang_ptm=202404 and loaihd_id=1 and thuebao_id=a.thuebao_id)
						or exists(select 1 from ttkd_bct.ptm_codinh_202404 where loaihd_id=1 and  thuebao_id=a.thuebao_id)
					)
			 ;


	update ttkd_bct.thaydoitocdo_202404 
	    set lydo_khongtinh_luong=lydo_khongtinh_luong || 'Chu quan khong thuoc TTKD HCM'
	    -- select * from ttkd_bct.thaydoitocdo_202404 
	    where chuquan_id not in (145,264,266)
	    ;
    
    
	update ttkd_bct.thaydoitocdo_202404 
	    set lydo_khongtinh_luong=lydo_khongtinh_luong || 'Cuoc phat sinh sau khi doi toc do khong cao hon'
	    -- select * from ttkd_bct.thaydoitocdo_202404
	    where chuquan_id in (145,264,266) and nvl(dthu_ps_new,0)<=nvl(dthu_ps_old,0)
				--	and (THUEBAO_ID, HDTB_ID) IN (SELECT  THUEBAO_ID, HDTB_ID FROM hocnq_ttkd.ptm_xuly_khieunai_tocdo_202404)

    ;
    commit;
    
        
alter table thaydoitocdo_202403
                add (manv_ptm varchar2(20), tennv_ptm varchar2(100), pbh_ptm_id number(3),
                        ma_pb varchar2(20),ten_pb varchar2(100),ma_to varchar2(20),ten_to varchar2(100),ma_vtcv varchar2(20),
                        loai_ld varchar2(100), manv_hotro varchar2(20), tyle_hotro number(5,2), tyle_am number(6,2), nhom_tiepthi number );

update thaydoitocdo_202403 a 
    set pbh_ptm_id='',manv_ptm='',tennv_ptm='',ma_to='',ten_to='',ma_pb='',ten_pb='',ma_vtcv='',loai_ld='',nhom_tiepthi='';
    
update thaydoitocdo_202403 a set (pbh_ptm_id,manv_ptm,tennv_ptm,ma_to,ten_to,ma_pb,ten_pb,ma_vtcv,loai_ld,nhom_tiepthi)=
          (select pb.pbh_id,b.ma_nv,b.ten_nv,b.ma_to,b.ten_to,b.ma_pb,b.ten_pb,b.ma_vtcv,b.loai_ld,nhomld_id
                     from ttkd_bsc.nhanvien_202403 b, ttkd_bsc.dm_phongban pb 
                    where b.ma_pb=pb.ma_pb and pb.active=1 and b.ma_nv=a.ma_tiepthi_new)
    where exists(select 1 from ttkd_bsc.nhanvien_202403 b where b.manv_hrm=a.ma_tiepthi_new);
     
-- Khong update cho TTVT vi chi tinh thu lao ptm cho TTVT

-- Manv ho tro:
update thaydoitocdo_202403 a set manv_hotro='', tyle_hotro='', tyle_am='';
update thaydoitocdo_202403 a set (manv_hotro, tyle_hotro, tyle_am)=
                (select distinct c.manv_presale_hrm, c.tyle/100, decode(tyle_am,0,1,c.tyle_am/100)
                from ttkdhcm_ktnv.amas_yeucau_dichvu b, ttkdhcm_ktnv.amas_booking_presale c
                where b.ma_yeucau = c.ma_yeucau and b.id_ycdv = c.id_ycdv  
                            and c.tyle > 0 and ps_truong = 1 and xacnhan = 1
                            and c.ngaycapnhat = (select max(c1.ngaycapnhat) from ttkdhcm_ktnv.amas_yeucau_dichvu b1, ttkdhcm_ktnv.amas_booking_presale c1
                                                                where b1.ma_yeucau=c1.ma_yeucau and b1.id_ycdv=c1.id_ycdv and c1.tyle>0 and c1.ps_truong=1 and c1.xacnhan=1 
                                                                            and b1.ma_yeucau=b.ma_yeucau and b1.ma_dichvu=b.ma_dichvu)           
                            and b.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', ''))
                            and b.ma_dichvu = a.loaitb_id
                               )
    where ma_duan_banhang is not null 
                and exists (select distinct c.manv_presale_hrm, c.tyle/100
                                        from ttkdhcm_ktnv.amas_yeucau_dichvu b, ttkdhcm_ktnv.amas_booking_presale c
                                        where b.ma_yeucau = c.ma_yeucau and b.id_ycdv = c.id_ycdv  
                                                    and c.tyle > 0 and ps_truong = 1 and xacnhan = 1
                                                    and c.ngaycapnhat = (select max(c1.ngaycapnhat) from ttkdhcm_ktnv.amas_yeucau_dichvu b1, ttkdhcm_ktnv.amas_booking_presale c1
                                                                                          where b1.ma_yeucau=c1.ma_yeucau and b1.id_ycdv=c1.id_ycdv and c1.tyle>0 and c1.ps_truong=1 and c1.xacnhan=1 
                                                                                                    and b1.ma_yeucau=b.ma_yeucau and b1.ma_dichvu=b.ma_dichvu)           
                                                    and b.ma_yeucau = to_number(regexp_replace (a.ma_duan_banhang, '\D', ''))
                                                    and b.ma_dichvu = a.loaitb_id
                                                       );
                                                       

select * from ttkd_bct.thaydoitocdo_202404;
select * from ttkd_bct.thaydoitocdo_202404_goc