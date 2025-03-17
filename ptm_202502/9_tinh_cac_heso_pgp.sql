create table ttkd_bsc.ct_bsc_ptm_pgp_202406_l1 as select * from ttkd_bsc.ct_bsc_ptm_pgp;
create table ttkd_bsc.ct_bsc_ptm_pgp as select * from ttkd_bsc.ct_bsc_ptm_pgp_202404;
update ttkd_bsc.ct_bsc_ptm_pgp a set THANG_TLDG_DT_NVHOTRO = 202502, THANG_TLKPI_HOTRO = 202502
		where rowid = 'AAI4xGAARAAAsBlAAE';
		commit;
create table ttkd_bsc.ct_bsc_ptm_pgp as 
			select PTM_ID, THANG_PTM, NGUON, MA_DUAN, MA_GD, MA_TB, MA_KH, DICH_VU, DICHVUVT_ID
			, LOAITB_ID, TENKIEU_LD, KIEULD_ID, KHACHHANG_ID, THANHTOAN_ID, THUEBAO_ID, HDKH_ID, HDTB_ID
			, LOAIHD_ID, TEN_TB, DIACHI_LD, SO_GT, MST, MST_TT, NGAY_BBBG, TRANGTHAITB_ID, TRANGTHAITB_ID_N1
			, TRANGTHAITB_ID_N2, TRANGTHAITB_ID_N3, NOCUOC_PTM, NOCUOC_N1, NOCUOC_N2, NOCUOC_N3, MA_TIEPTHI
			, MA_NGUOIGT, NGUOI_GT, MANV_PTM, TENNV_PTM, MA_TO, MANV_HOTRO, TYLE_HOTRO, TYLE_HOTRO_NV, GHI_CHU
			, LYDO_KHONGTINH_LUONG, DTHU_PS, DTHU_GOI, DTHU_GOI_NGOAIMANG, DOITUONG_KH, KHHH_KHM, DIABAN, PHANLOAI_KH
			, HESO_KHACHHANG, HESO_DICHVU, HESO_DICHVU_1, HESO_TRATRUOC, HESO_KHUYENKHICH, HESO_TBNGANHAN, HESO_KVDACTHU
			, HESO_VTCV_NVPTM, TYLE_HUONGDT, HESO_VTCV_NVHOTRO, HESO_HOTRO_NVPTM, HESO_HOTRO_NVHOTRO, HESO_QUYDINH_NVHOTRO
			, HESO_DIABAN_TINHKHAC, nvl(HESO_DIABAN_TINHKHAC, null) heso_daily, decode(thang_ptm, 202407, 800, 858) dongia, DOANHTHU_KPI_NVPTM, DOANHTHU_KPI_NVHOTRO, DOANHTHU_DONGIA_NVHOTRO, LUONG_DONGIA_NVHOTRO
			, THANG_TLDG_DT_NVHOTRO, THANG_TLKPI_HOTRO, LYDO_KHONGTINH_DONGIA, DT_DONGIA_PGP, LG_DONGIA_PGP, DT_KPI_PGP, TOCDO_ID, MA_DUAN_BANHANG, NGAY_INS 
			from ttkd_bsc.x_ct_bsc_ptm_pgp; ---backup truoc khi xy ly 43 tbao nghi ngo sai

rename ct_bsc_ptm_pgp to x_ct_bsc_ptm_pgp;
drop table ttkd_bsc.x_ct_bsc_ptm_pgp purge;

rollback;
commit;
select * from ttkd_bsc.ct_bsc_ptm_pgp where trunc(ngay_ins) >= '01/03/2025'; 
select * from ttkd_bsc.ct_bsc_ptm_pgp where thang_tldg_dt_nvhotro = 202408; and heso_daily = 0.05;
select * from ttkd_bsc.ct_bsc_ptm_pgp where thang_ptm = 202502; and hdtb_id in (26277035, 26276940); or ngay_ins >= '01/09/2024'; and heso_daily = 0.05; and ma_tb in ('hcm_hddt_00013102', 'hcm_hddt_00023066', 'hcm_hddt_00007104')
							and thang_ptm = 202406; 
							
select * from ttkd_bsc.ct_bsc_ptm where ma_gd = 'HCM-LD/01791391';
select * from ttkd_bsc.ct_bsc_ptm where  ma_duan_banhang = '282078';
select * from ttkd_bsc.ct_bsc_ptm where ma_tb = 'hcm_hddt_mtt_00000342';

select * from ttkd_bsc.nhanvien where thang = 202502 and ma_nv = 'VNP001686';

--delete from ttkd_bsc.ct_bsc_ptm_pgp where trunc(ngay_ins) >= '01/03/2025'; 
--delete from ttkd_bsc.ct_bsc_ptm_pgp where thang_ptm = 202502 and ma_tb = 'hcm_ioff_00000672';; and ngay_ins = '04/11/2024 12:27:39'; 

----***check 1 duan án, 1 tbao mà tỷ lê 1 nhóm PGP vượt 100%.
select ma_duan_banhang, ptm_id from ttkd_bsc.ct_bsc_ptm_pgp where trunc(ngay_ins) >= '01/01/2025' group by ma_duan_banhang, ptm_id having sum(TYLE_HOTRO_NV) >1;
select ma_duan_banhang, ptm_id from ttkd_bsc.ct_bsc_ptm_pgp where thang_ptm = 202502 group by ma_duan_banhang, ptm_id having count(TYLE_HOTRO_NV) =2;
----***END check;
select rowid, a.* from ttkd_bsc.ct_bsc_ptm_pgp a where ma_tb = 'hcm_edu_00009998';
select * from ttkd_bsc.ct_bsc_ptm_pgp where ma_duan_banhang = '367200';
select * from ttkd_bsc.ct_bsc_ptm_pgp where ma_gd = '01101455';
--delete from ttkd_bsc.ct_bsc_ptm_pgp where  ma_duan_banhang = '307441' and manv_hotro = 'VNP028491' and NGAY_INS ='20/11/2024 09:54:45';
        
	   
	   
	   	---code moi
		delete from ttkd_bsc.ct_bsc_ptm_pgp a
--		select * from ttkd_bsc.ct_bsc_ptm_pgp a
			where exists (select * from ttkd_bsc.ct_bsc_ptm where ma_duan_banhang is not null and thang_tldg_dt_nvhotro = 202502 and a.ptm_id = id and luong_dongia_nvhotro <> a.LG_DONGIA_PGP)
						and THANG_TLDG_DT_NVHOTRO is not null 
						and thang_ptm = 202502
--						and ma_duan_banhang = '00882474'
		;
insert into ttkd_bsc.ct_bsc_ptm_pgp a
							  (ptm_id, thang_ptm, nguon, ma_gd, ma_kh, ma_tb, dich_vu, dichvuvt_id, loaitb_id, tenkieu_ld, kieuld_id, khachhang_id, thanhtoan_id, thuebao_id
							   ,hdkh_id, hdtb_id, loaihd_id, ten_tb,diachi_ld, so_gt, mst, mst_tt, ma_duan_banhang, ngay_bbbg
							   ,trangthaitb_id, trangthaitb_id_n1, trangthaitb_id_n2, trangthaitb_id_n3, nocuoc_ptm, nocuoc_n1, nocuoc_n2, nocuoc_n3
							   ,ma_tiepthi, ma_nguoigt, nguoi_gt,manv_ptm, tennv_ptm, ma_to, manv_hotro, tyle_hotro, tyle_hotro_nv
							   ,ghi_chu, lydo_khongtinh_luong, dthu_ps, dthu_goi, dthu_goi_ngoaimang, doituong_kh, khhh_khm
							   ,diaban, phanloai_kh, heso_khachhang, heso_dichvu, heso_dichvu_1, heso_tratruoc
							   ,heso_tbnganhan, heso_kvdacthu, heso_vtcv_nvptm, tyle_huongdt
							   ,heso_vtcv_nvhotro, heso_hotro_nvptm, heso_hotro_nvhotro,heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_daily, dongia, doanhthu_kpi_nvptm
							   ,doanhthu_kpi_nvhotro, doanhthu_dongia_nvhotro, luong_dongia_nvhotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia
							   ,dt_dongia_pgp, lg_dongia_pgp, dt_kpi_pgp )

with 
			yc_dv as (select ma_yeucau, id_ycdv, ma_dichvu, row_number() over(partition by MA_YEUCAU, MA_DICHVU order by NGAYCAPNHAT desc) rnk
							from ttkdhcm_ktnv.amas_yeucau_dichvu
							where MA_HIENTRANG <> 14
							)
			, ta as	 (select c.manv_presale_hrm, c.tyle/100 tyle_hotro, decode(tyle_am,0,1,c.tyle_am/100) tyle_am, d.loaitb_id_obss, b.ma_yeucau, b.ma_dichvu, c.tyle_nhom, c.tyle--, c.id_ycdv, c.tyle_am tyle_am_goc
								, c.NGAYHEN, c.NGAYCAPNHAT, c.NGAYNHANTIN_PS, c.NGAYXACNHAN, c.ps_truong
						 from yc_dv b, ttkdhcm_ktnv.amas_booking_presale c, ttkdhcm_ktnv.amas_loaihinh_tb d
											where b.ma_yeucau=c.ma_yeucau and b.id_ycdv=c.id_ycdv and b.ma_dichvu = d.loaitb_id
													   and c.xacnhan=1  
						)
			, tt as (select MANV_PRESALE_HRM, LOAITB_ID_OBSS, MA_YEUCAU, MA_DICHVU, TYLE_AM--, sum(tyle_hotro) tyle_hotro, sum(TYLE_NHOM) TYLE_NHOM
									, decode(sum(ps_truong), 1, sum(tyle_hotro), max(tyle_hotro)) tyle_hotro
									, decode(sum(ps_truong), 1, sum(TYLE_NHOM), max(TYLE_NHOM)) TYLE_NHOM
						from ta --where ma_yeucau in (199069, 243885)
						group by MANV_PRESALE_HRM, LOAITB_ID_OBSS, MA_YEUCAU, MA_DICHVU, TYLE_AM
						)
--		select*
--		from tt where ma_yeucau in ('325103', '282078')
select id, thang_ptm, nguon, ma_gd, ma_kh, ma_tb, dich_vu, dichvuvt_id, loaitb_id, tenkieu_ld, kieuld_id 
				   ,khachhang_id, thanhtoan_id, thuebao_id, hdkh_id, hdtb_id, loaihd_id, ten_tb,diachi_ld, so_gt, mst, mst_tt, ma_duan_banhang, ngay_bbbg
				   ,trangthaitb_id, trangthaitb_id_n1, trangthaitb_id_n2, trangthaitb_id_n3, nocuoc_ptm, nocuoc_n1, nocuoc_n2, nocuoc_n3
				   ,ma_tiepthi, ma_nguoigt, nguoi_gt,manv_ptm, tennv_ptm, ma_to
				   , case when b.manv_presale_hrm is not null then b.manv_presale_hrm else a.manv_hotro end manv_hotro
				   , a.tyle_hotro
				   , case when b.manv_presale_hrm is not null then round(b.tyle_nhom/(a.tyle_hotro*100),2) else 1 end tyle_hotro_nv
				   ,ghi_chu, lydo_khongtinh_luong, dthu_ps, dthu_goi, dthu_goi_ngoaimang, doituong_kh, khhh_khm
				   ,diaban, phanloai_kh, heso_khachhang, heso_dichvu, heso_dichvu_1, heso_tratruoc
				   ,heso_tbnganhan, heso_kvdacthu, heso_vtcv_nvptm, tyle_huongdt
				   ,heso_vtcv_nvhotro, heso_hotro_nvptm, heso_hotro_nvhotro,heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_daily, dongia, doanhthu_kpi_nvptm
				   ,round(doanhthu_kpi_nvhotro * round(b.tyle_nhom/(a.tyle_hotro*100),2) ,0) doanhthu_kpi_nvhotro
				   ,round(doanhthu_dongia_nvhotro * round(b.tyle_nhom/(a.tyle_hotro*100),2) ,0) doanhthu_dongia_nvhotro
				   ,round(luong_dongia_nvhotro * round(b.tyle_nhom/(a.tyle_hotro*100),2) ,0) luong_dongia_nvhotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia
				   ,doanhthu_dongia_nvhotro dt_dongia_pgp, luong_dongia_nvhotro lg_dongia_pgp, doanhthu_kpi_nvhotro dt_kpi_gp
    from ttkd_bsc.ct_bsc_ptm a
				left join tt b on to_number(regexp_replace (a.ma_duan_banhang, '\D', ''))=b.ma_yeucau and a.loaitb_id=b.loaitb_id_obss
    where thang_ptm = 202502 and manv_hotro is not null and a.ma_duan_banhang is not null and (loaitb_id is null or loaitb_id<>21)			--thang n
					  and exists(select ten_pb, thang from ttkd_bsc.nhanvien where (ma_pb='VNP0702600' or ma_nv = 'VNP017718') and thang= a.thang_ptm and ma_nv=a.manv_hotro)
					  and not exists(select 1 from ttkd_bsc.ct_bsc_ptm_pgp where ptm_id = a.id)
--				 and a.ma_duan_banhang = '277600'
			--	and thang_luong = 87 ---loai ds imp_ctr
--			and ma_tb = 'hcm_tdth_00000296'
--and ma_gd = 'HCM-LD/01939638'
		  ;
		commit ;
		rollback;
-- Cap nhat lai cac hop dong dc tinh bs: chay sau khi chay xet_thangtlkpi
		---code moi
		delete from ttkd_bsc.ct_bsc_ptm_pgp a
--		select * from ttkd_bsc.ct_bsc_ptm_pgp a
			where exists (select * from ttkd_bsc.ct_bsc_ptm where ma_duan_banhang is not null and thang_tldg_dt_nvhotro = 202502 and a.ptm_id = id )
						and THANG_TLDG_DT_NVHOTRO is null 
						and thang_ptm < 202502 
--						and ma_duan_banhang = '00882474'
		;
		delete from ttkd_bsc.ct_bsc_ptm_pgp a
--		select * from ttkd_bsc.ct_bsc_ptm_pgp a
			where exists (select * from ttkd_bsc.ct_bsc_ptm where ma_duan_banhang is not null and thang_tlkpi_hotro = 202502 and a.ptm_id = id )
						and THANG_TLKPI_HOTRO is null 
						and thang_ptm < 202502
		;
		select * from ttkd_bsc.ct_bsc_ptm_pgp a
			where ptm_id in (select ptm_id from ttkd_bsc.ct_bsc_ptm_pgp group by ptm_id having count(*)>1)
						and thang_ptm >=202502
			;
		----insert thuebao tinh bsung
		insert into ttkd_bsc.ct_bsc_ptm_pgp a
							  (ptm_id, thang_ptm, nguon, ma_gd, ma_kh, ma_tb, dich_vu, dichvuvt_id, loaitb_id, tenkieu_ld, kieuld_id, khachhang_id, thanhtoan_id, thuebao_id
							   ,hdkh_id, hdtb_id, loaihd_id, ten_tb,diachi_ld, so_gt, mst, mst_tt, ma_duan_banhang, ngay_bbbg
							   ,trangthaitb_id, trangthaitb_id_n1, trangthaitb_id_n2, trangthaitb_id_n3, nocuoc_ptm, nocuoc_n1, nocuoc_n2, nocuoc_n3
							   ,ma_tiepthi, ma_nguoigt, nguoi_gt,manv_ptm, tennv_ptm, ma_to, manv_hotro, tyle_hotro, tyle_hotro_nv
							   ,ghi_chu, lydo_khongtinh_luong, dthu_ps, dthu_goi, dthu_goi_ngoaimang, doituong_kh, khhh_khm
							   ,diaban, phanloai_kh, heso_khachhang, heso_dichvu, heso_dichvu_1, heso_tratruoc, heso_khuyenkhich
							   ,heso_tbnganhan, heso_kvdacthu, heso_vtcv_nvptm, tyle_huongdt
							   ,heso_vtcv_nvhotro, heso_hotro_nvptm, heso_hotro_nvhotro,heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_daily, dongia, doanhthu_kpi_nvptm
							   ,doanhthu_kpi_nvhotro, doanhthu_dongia_nvhotro, luong_dongia_nvhotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia
							   ,dt_dongia_pgp, lg_dongia_pgp, dt_kpi_pgp )

		with 
			yc_dv as (select ma_yeucau, id_ycdv, ma_dichvu, row_number() over(partition by MA_YEUCAU, MA_DICHVU order by NGAYCAPNHAT desc) rnk
							from ttkdhcm_ktnv.amas_yeucau_dichvu
							where MA_HIENTRANG <> 14
							)
			, ta as	 (select c.manv_presale_hrm, c.tyle/100 tyle_hotro, decode(tyle_am,0,1,c.tyle_am/100) tyle_am, d.loaitb_id_obss, b.ma_yeucau, b.ma_dichvu, c.tyle_nhom--, c.id_ycdv, c.tyle_am tyle_am_goc
								, c.NGAYHEN, c.NGAYCAPNHAT, c.NGAYNHANTIN_PS, c.NGAYXACNHAN, c.ps_truong
						 from yc_dv b, ttkdhcm_ktnv.amas_booking_presale c, ttkdhcm_ktnv.amas_loaihinh_tb d
											where b.ma_yeucau=c.ma_yeucau and b.id_ycdv=c.id_ycdv and b.ma_dichvu = d.loaitb_id
													   and c.xacnhan=1  
						)
			, tt as (select MANV_PRESALE_HRM, LOAITB_ID_OBSS, MA_YEUCAU, MA_DICHVU, TYLE_AM--, sum(tyle_hotro) tyle_hotro, sum(TYLE_NHOM) TYLE_NHOM
								, decode(sum(ps_truong), 1, sum(tyle_hotro), max(tyle_hotro)) tyle_hotro
								, decode(sum(ps_truong), 1, sum(TYLE_NHOM), max(TYLE_NHOM)) TYLE_NHOM
						from ta 
--						where ma_yeucau in (282078)
						group by MANV_PRESALE_HRM, LOAITB_ID_OBSS, MA_YEUCAU, MA_DICHVU, TYLE_AM
						)
--											select * from yc_dv where ma_yeucau in ('297311')
											
					select --a.loaitb_id
									id, thang_ptm, nguon, ma_gd, ma_kh, ma_tb, dich_vu, dichvuvt_id, a.loaitb_id, tenkieu_ld, kieuld_id 
									   ,khachhang_id, thanhtoan_id, thuebao_id, hdkh_id, hdtb_id, loaihd_id, ten_tb,diachi_ld, so_gt, mst, mst_tt, ma_duan_banhang, ngay_bbbg
									   ,trangthaitb_id, trangthaitb_id_n1, trangthaitb_id_n2, trangthaitb_id_n3, nocuoc_ptm, nocuoc_n1, nocuoc_n2, nocuoc_n3
									   ,ma_tiepthi, ma_nguoigt, nguoi_gt,manv_ptm, tennv_ptm, ma_to
									   , case when b.manv_presale_hrm is not null then b.manv_presale_hrm else a.manv_hotro end manv_hotro
									   , a.tyle_hotro
									   , case when b.manv_presale_hrm is not null then round(b.tyle_nhom/(a.tyle_hotro*100),2) else 1 end tyle_hotro_nv
									   ,ghi_chu, lydo_khongtinh_luong, dthu_ps, dthu_goi, dthu_goi_ngoaimang, doituong_kh, khhh_khm
									   ,diaban, phanloai_kh, heso_khachhang, heso_dichvu, heso_dichvu_1, heso_tratruoc, heso_khuyenkhich
									   ,heso_tbnganhan, heso_kvdacthu, heso_vtcv_nvptm, tyle_huongdt
									   ,heso_vtcv_nvhotro, heso_hotro_nvptm, heso_hotro_nvhotro,heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_daily, dongia, doanhthu_kpi_nvptm
									   ,round(doanhthu_kpi_nvhotro * round(b.tyle_nhom/(a.tyle_hotro*100),2) ,0) doanhthu_kpi_nvhotro
									   ,round(doanhthu_dongia_nvhotro * round(b.tyle_nhom/(a.tyle_hotro*100),2) ,0) doanhthu_dongia_nvhotro        
									   ,round(luong_dongia_nvhotro * round(b.tyle_nhom/(a.tyle_hotro*100),2) ,0) luong_dongia_nvhotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia
									   ,doanhthu_dongia_nvhotro dt_dongia_pgp, luong_dongia_nvhotro lg_dongia_pgp, doanhthu_kpi_nvhotro dt_kpi_gp
					    from ttkd_bsc.ct_bsc_ptm a
									left join tt b on to_number(regexp_replace (a.ma_duan_banhang, '\D', ''))  = b.ma_yeucau and a.loaitb_id = b.loaitb_id_obss
					    where (thang_tldg_dt_nvhotro = 202502 or thang_tlkpi_hotro = 202502)  and manv_hotro is not null and a.ma_duan_banhang is not null and (loaitb_id is null or loaitb_id<>21)			--thang n
										  and exists(select 1 from ttkd_bsc.nhanvien where thang = 202502 and (ma_pb='VNP0702600' or ma_nv = 'VNP017718' ) and ma_nv=a.manv_hotro)
										  and not exists (select ptm_id from ttkd_bsc.ct_bsc_ptm_pgp where ptm_id is not null and ptm_id = a.id)
--										  and a.ma_duan_banhang = '297311'
--											and a.ma_tb = 'hcm_ca_plugin_00008834'
--											and a.manv_hotro = 'VNP028491'

			;		  
		---end code moi
		;


            commit;
		
		   ;
                    
