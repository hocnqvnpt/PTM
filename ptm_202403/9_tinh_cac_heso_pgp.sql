create table ttkd_bsc.ct_bsc_ptm_pgp_202403 as select * from ttkd_bsc.ct_bsc_ptm_pgp;

rollback;
select * from ttkd_bsc.ct_bsc_ptm_pgp where thang_tldg_dt_nvhotro=202404; ;

delete from ttkd_bsc.ct_bsc_ptm_pgp where thang_ptm=202404; 
        
insert into ttkd_bsc.ct_bsc_ptm_pgp a
							  (ptm_id, thang_ptm, nguon, ma_duan, ma_gd, ma_tb, dich_vu, dichvuvt_id, loaitb_id, tenkieu_ld, kieuld_id, khachhang_id, thanhtoan_id, thuebao_id
							   ,hdkh_id, hdtb_id, loaihd_id, ten_tb,diachi_ld, so_gt, mst, mst_tt, ma_duan_banhang, ngay_bbbg
							   ,trangthaitb_id, trangthaitb_id_n1, trangthaitb_id_n2, trangthaitb_id_n3, nocuoc_ptm, nocuoc_n1, nocuoc_n2, nocuoc_n3
							   ,ma_tiepthi, ma_nguoigt, nguoi_gt,manv_ptm, tennv_ptm, ma_to, manv_hotro, tyle_hotro, tyle_hotro_nv
							   ,ghi_chu, lydo_khongtinh_luong, dthu_ps, dthu_goi, dthu_goi_ngoaimang, doituong_kh, khhh_khm
							   ,diaban, phanloai_kh, heso_khachhang, heso_dichvu, heso_dichvu_1, heso_tratruoc, heso_khuyenkhich
							   ,heso_tbnganhan, heso_kvdacthu, heso_vtcv_nvptm, tyle_huongdt
							   ,heso_vtcv_nvhotro, heso_hotro_nvptm, heso_hotro_nvhotro,heso_quydinh_nvhotro, heso_diaban_tinhkhac, doanhthu_kpi_nvptm
							   ,doanhthu_kpi_nvhotro, doanhthu_dongia_nvhotro, luong_dongia_nvhotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia
							   ,dt_dongia_pgp, lg_dongia_pgp, dt_kpi_pgp )
--								id, thang_ptm, nguon, ma_duan, ma_gd, ma_tb, dich_vu, dichvuvt_id, loaitb_id, tenkieu_ld, kieuld_id, khachhang_id, thanhtoan_id, thuebao_id
--								, hdkh_id, hdtb_id, loaihd_id, ten_tb, diachi_ld, so_gt, mst, mst_tt, ma_duan_banhang, ngay_bbbg
--								, trangthaitb_id, trangthaitb_id_n1, trangthaitb_id_n2, trangthaitb_id_n3, nocuoc_ptm, nocuoc_n1, nocuoc_n2, nocuoc_n3
--								, ma_tiepthi, ma_nguoigt, nguoi_gt, manv_ptm, tennv_ptm, ma_to, manv_hotro, tyle_hotro, tyle_hotro_nv
--								, ghi_chu, lydo_khongtinh_luong, dthu_ps, dthu_goi, dthu_goi_ngoaimang, doituong_kh, khhh_khm
--								, diaban, phanloai_kh, heso_khachhang, heso_dichvu, heso_dichvu_1, heso_tratruoc, heso_khuyenkhich
--								, heso_tbnganhan, heso_kvdacthu, heso_vtcv_nvptm, tyle_huongdt
--								, heso_vtcv_nvhotro, heso_hotro_nvptm, heso_hotro_nvhotro, heso_quydinh_nvhotro, heso_diaban_tinhkhac, doanhthu_kpi_nvptm
--								, doanhthu_kpi_nvhotro, doanhthu_dongia_nvhotro, luong_dongia_nvhotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia
--								, dt_dongia_pgp, lg_dongia_pgp, dt_kpi_gp					   
;
with 
			yc_dv as (select ma_yeucau, id_ycdv, ma_dichvu, row_number() over(partition by MA_YEUCAU, MA_DICHVU order by NGAYCAPNHAT desc) rnk
							from ttkdhcm_ktnv.amas_yeucau_dichvu
							)
			, t as	 (select c.manv_presale_hrm, c.tyle/100 tyle_hotro, decode(tyle_am,0,1,c.tyle_am/100) tyle_am, d.loaitb_id_obss, b.ma_yeucau, b.ma_dichvu, c.tyle_nhom--, c.id_ycdv, c.tyle_am tyle_am_goc
								, NGAYHEN, NGAYCAPNHAT, NGAYNHANTIN_PS, NGAYXACNHAN
						 from yc_dv b, ttkdhcm_ktnv.amas_booking_presale c, ttkdhcm_ktnv.amas_loaihinh_tb d
											where b.rnk = 1 and b.ma_yeucau=c.ma_yeucau and b.id_ycdv=c.id_ycdv and b.ma_dichvu = d.loaitb_id
													   and c.xacnhan=1  
						)
		--select distinct t.manv_presale_hrm, t.tyle_hotro, t.tyle_am, ma_yeucau, loaitb_id_obss, id_ycdv, tyle_am_goc
		--from t where ma_yeucau= 162833
select id, thang_ptm, nguon, ma_duan_banhang ma_duan, ma_gd, ma_tb, dich_vu, dichvuvt_id, loaitb_id, tenkieu_ld, kieuld_id 
				   ,khachhang_id, thanhtoan_id, thuebao_id, hdkh_id, hdtb_id, loaihd_id, ten_tb,diachi_ld, so_gt, mst, mst_tt, ma_duan_banhang, ngay_bbbg
				   ,trangthaitb_id, trangthaitb_id_n1, trangthaitb_id_n2, trangthaitb_id_n3, nocuoc_ptm, nocuoc_n1, nocuoc_n2, nocuoc_n3
				   ,ma_tiepthi, ma_nguoigt, nguoi_gt,manv_ptm, tennv_ptm, ma_to
				  -- ,b.manv_presale_hrm manv_hotro, a.tyle_hotro, round(b.tyle_nhom/(a.tyle_hotro*100),2) tyle_hotro_nv
				   ---b.manv_presale_hrm --> not null thi lay no, nguoc lay a.manv_hotro file GOM
				   ---tuong tu tyle_hotro_nv
--				   , b.manv_presale_hrm
				   , case when b.manv_presale_hrm is not null then b.manv_presale_hrm else a.manv_hotro end manv_hotro
				   , a.tyle_hotro
				   , case when b.manv_presale_hrm is not null then round(b.tyle_nhom/(a.tyle_hotro*100),2) else 1 end tyle_hotro_nv
				   ,ghi_chu, lydo_khongtinh_luong, dthu_ps, dthu_goi, dthu_goi_ngoaimang, doituong_kh, khhh_khm
				   ,diaban, phanloai_kh, heso_khachhang, heso_dichvu, heso_dichvu_1, heso_tratruoc, heso_khuyenkhich
				   ,heso_tbnganhan, heso_kvdacthu, heso_vtcv_nvptm, tyle_huongdt
				   ,heso_vtcv_nvhotro, heso_hotro_nvptm, heso_hotro_nvhotro,heso_quydinh_nvhotro, heso_diaban_tinhkhac, doanhthu_kpi_nvptm
				   ,round(doanhthu_kpi_nvhotro * round(b.tyle_nhom/(a.tyle_hotro*100),2) ,0) doanhthu_kpi_nvhotro
				   ,round(doanhthu_dongia_nvhotro * round(b.tyle_nhom/(a.tyle_hotro*100),2) ,0) doanhthu_dongia_nvhotro        
				   ,round(luong_dongia_nvhotro * round(b.tyle_nhom/(a.tyle_hotro*100),2) ,0) luong_dongia_nvhotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia
				   ,doanhthu_dongia_nvhotro dt_dongia_pgp, luong_dongia_nvhotro lg_dongia_pgp, doanhthu_kpi_nvhotro dt_kpi_gp
    from ttkd_bsc.ct_bsc_ptm a
--              left join (select distinct b.ma_yeucau, c.id_ycdv, b.manv_presale_hrm, b.tyle_nhom, b.tyle, c.ma_dichvu, d.loaitb_id_obss
--                                from ttkdhcm_ktnv.amas_booking_presale b, ttkdhcm_ktnv.amas_yeucau_dichvu c,  ttkdhcm_ktnv.amas_loaihinh_tb d
--                                where b.id_ycdv = c.id_ycdv and c.ma_dichvu=d.loaitb_id and b.xacnhan=1 
--                                ) b on a.ma_duan_banhang=b.ma_yeucau and a.loaitb_id=b.loaitb_id_obss
				left join t b on a.ma_duan_banhang=b.ma_yeucau and a.loaitb_id=b.loaitb_id_obss
    where thang_ptm=202404 and manv_hotro is not null and (loaitb_id is null or loaitb_id<>21)			--thang n
					  and exists(select 1 from ttkd_bsc.nhanvien_202404 where ma_pb='VNP0702600' and ma_nv=a.manv_hotro)
					  and not exists(select 1 from ttkd_bsc.ct_bsc_ptm_pgp where ptm_id=a.id)
				 --and a.ma_duan_banhang = '206876'
				--and thang_luong <> 86 ---loai ds imp_ctr
		  ;
 commit
 ;             
              
-- Cap nhat lai cac hop dong dc tinh bs: chay sau khi chay xet_thangtlkpi
			update ttkd_bsc.ct_bsc_ptm_pgp a
			    set (trangthaitb_id, trangthaitb_id_n1, trangthaitb_id_n2, trangthaitb_id_n3, 
					  nocuoc_ptm, nocuoc_n1, nocuoc_n2, nocuoc_n3,thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia,
					  doanhthu_kpi_nvhotro, doanhthu_dongia_nvhotro, luong_dongia_nvhotro)
					= (select trangthaitb_id, trangthaitb_id_n1, trangthaitb_id_n2, trangthaitb_id_n3, 
							    nocuoc_ptm, nocuoc_n1, nocuoc_n2, nocuoc_n3,thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia,
							    doanhthu_kpi_nvhotro, doanhthu_dongia_nvhotro, luong_dongia_nvhotro
					  from ttkd_bsc.ct_bsc_ptm
					  where id = a.ptm_id)
				 -- select * from  ttkd_bsc.ct_bsc_ptm_pgp a
				   where thang_ptm>=202401			---thang n-3
								and (thang_tldg_dt_nvhotro is null or thang_tldg_dt_nvhotro=202404)	---thang n
--								and a.ma_duan_banhang = '206876'
								
				   ;
            
        
	update ttkd_bsc.ct_bsc_ptm_pgp a 
		   set doanhthu_dongia_nvhotro=round(dt_dongia_pgp*tyle_hotro_nv ,0), 
			    luong_dongia_nvhotro=round(lg_dongia_pgp*tyle_hotro_nv,0),
			    doanhthu_kpi_nvhotro=round(dt_kpi_pgp*tyle_hotro_nv,0)
		   -- select thang_ptm, ma_tb, manv_hotro, tyle_hotro, tyle_hotro_nv,doanhthu_dongia_nvhotro,luong_dongia_nvhotro, doanhthu_kpi_nvhotro
				, dt_dongia_pgp, lg_dongia_pgp, dt_kpi_pgp, lydo_khongtinh_dongia   from ttkd_bsc.ct_bsc_ptm_pgp a
		   where thang_ptm>202401 		---thang n-3
						and (thang_tldg_dt_nvhotro is null or thang_tldg_dt_nvhotro=202404) ---thang n
--						and a.ma_duan_banhang = '206876'
						
		   ;
                    
                    
		update ttkd_bsc.ct_bsc_ptm_pgp a
		    set thang_tldg_dt_nvhotro=202404, thang_tlkpi_hotro=202404 ---thang n
		    -- select thang_tldg_dt_nvhotro, thang_tlkpi_hotro from ttkd_bsc.ct_bsc_ptm_pgp a            
		    where thang_ptm>=202401 			---thang n-3
					and thang_tldg_dt_nvhotro is null
				  and lydo_khongtinh_dongia like '%Dai ly%' 		---don gia khong tinh nhan vien AM QLDL (ngo?i tru vi tri AM thuong), ho tro thi tinh neu Dai ly PTM
				  and lydo_khongtinh_dongia not like '%No cuoc%' and lydo_khongtinh_dongia not like '%Trang thai%' and lydo_khongtinh_dongia not like '%doanh thu%'
--				  and a.ma_duan_banhang = '206876'
				   ;
            
 commit;
    
select thang_ptm, ma_gd, ma_tb, thang_ptm, lydo_khongtinh_dongia, dthu_ps from ttkd_bsc.ct_bsc_ptm_pgp a            
    where thang_ptm>=202401
            and thang_tldg_dt_nvhotro is null and lydo_khongtinh_dongia is null; 

select * from ttkd_bsc.ct_bsc_ptm_pgp a
    -- update ttkd_bsc.ct_bsc_ptm_pgp a set thang_tldg_dt_nvhotro= thang_tlkpi_hotro 
    where thang_tlkpi_hotro=202404 and thang_tldg_dt_nvhotro is null and lydo_khongtinh_dongia is null;


select thang_ptm, ma_gd, ma_tb, heso_dichvu, heso_khachhang, tyle_hotro, tyle_hotro_nv, dthu_goi, dt_dongia_pgp, lg_dongia_pgp,
            doanhthu_dongia_nvhotro, luong_dongia_nvhotro, dt_kpi_pgp, doanhthu_kpi_nvhotro
from ttkd_bsc.ct_bsc_ptm_pgp a
where thang_ptm=202404;


select * from ttkd_bsc.ct_bsc_ptm_pgp a
    -- update ttkd_bsc.ct_bsc_ptm_pgp set thang_tldg_dt_nvhotro='', thang_tlkpi_hotro=''
    where lydo_khongtinh_dongia is not null
        and ((thang_tldg_dt_nvhotro=202404 and thang_tlkpi_hotro is null)
                or (thang_tldg_dt_nvhotro is null and thang_tlkpi_hotro=202404));


select thang_ptm, ma_gd, ma_tb, loaitb_id, manv_hotro 
 from ttkd_bsc.ct_bsc_ptm_pgp
 where thang_ptm>=202401
 group by thang_ptm, ma_gd, ma_tb, loaitb_id, manv_hotro
 having count(*)>1;  
 
 delete from ttkd_bsc.ct_bsc_ptm_pgp where ptm_id=7552165;

-- va ins 2 row tu file temp_pgp_hcm_econtract_00000640 :
insert into ttkd_bsc.ct_bsc_ptm_pgp
    select * from ttkd_bsc.temp_pgp_hcm_econtract_00000640;
    commit;
    