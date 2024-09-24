commit;
----kiem tra khieu nai da tinh chu theo MA_TB----
	select id, thang_luong, thang_ptm, MA_NGUOIGT, NGUOI_GT, manv_ptm, tennv_ptm, ma_gd, dich_vu, loaitb_id, ma_tb, sothang_dc, LYDO_KHONGTINH_luong, LYDO_KHONGTINH_DONGIA
			, thang_bddc, dthu_ps_truoc, dthu_ps, dthu_goi, luong_dongia_nvptm, nguon, nop_du, mien_hsgoc, trangthai_tt_id, trangthaitb_id 
			, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong
			, ngay_luuhs_ttkd, ngay_luuhs_ttvt
		, tyle_huongdt, heso_dichvu, heso_dichvu_1, phanloai_kh, heso_khachhang, heso_tbnganhan, heso_tratruoc
		, heso_khuyenkhich, heso_kvdacthu, heso_vtcv_nvptm, heso_vtcv_dai, heso_vtcv_nvhotro
		, heso_hotro_nvptm, heso_hotro_dai, heso_hotro_nvhotro, heso_quydinh_nvptm, heso_quydinh_dai
		, heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_hoso, heso_dichvu_dnhm, heso_daily, dongia, doanhthu_dongia_nvptm
		, tyle_am, nhom_tiepthi
			from ttkd_bsc.ct_bsc_ptm
--			update ttkd_bsc.ct_bsc_ptm set thang_luong = 5, heso_hoso = 0.8, lydo_khongtinh_luong = null, manv_ptm = 'CTV083399'
						, tennv_ptm = 'Nguy?n Ng?c Ánh Ph??c', MA_VTCV = 'VNP-HNHCM_BHKV_6', MA_TO ='VNP07016H0', TEN_TO = 'T? Kinh Doanh ??a Bàn 1'
						, MA_PB ='VNP0701600', TEN_PB ='Phòng Bán Hàng Khu V?c Tân Bình', LOAI_LD ='Ctv Kd?', nhom_tiepthi = 3
						, vanban_id = 105552
					--	VNP-HNHCM_BHKV_6	Nhân Viên Kinh Doanh ??a Bàn	VNP07016H0	T? Kinh Doanh ??a Bàn 1	VNP0701600	Phòng Bán Hàng Khu V?c Tân Bình	Ctv Kd?b
								where-- thang_luong= 86
								
								thang_ptm >= 202301
							--	and ma_tb in ('hcm_ca_00097176') and MA_NGUOIGT in('GTGT00181', 'GTGT00182', 'GTGT00186', 'GTGT00194')
						--	and ma_tb in ('hcm_hddt_00005774')
							and ma_tb in ('84849827180');

		commit;
;

----kiem tra khieu nai da tinh chu theo MA_GD----
 select goi_cuoc, thang_luong, thang_ptm, MA_NGUOIGT, NGUOI_GT, manv_ptm, tennv_ptm, ma_gd, dich_vu, dichvuvt_id, loaitb_id, ma_tb, sothang_dc, LYDO_KHONGTINH_luong, LYDO_KHONGTINH_DONGIA
			, luong_dongia_nvptm, luong_dongia_nvhotro, nguon, nop_du, mien_hsgoc, trangthai_tt_id, trangthaitb_id, trangthaitb_id_n1
			, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong
			, ngay_luuhs_ttkd, ngay_luuhs_ttvt
			, thang_bddc, dthu_ps_truoc, dthu_ps, tien_dnhm, tien_sodep, dthu_goi, tyle_huongdt, heso_dichvu, heso_dichvu_1, khhh_khm, phanloai_kh, heso_khachhang, heso_tbnganhan, heso_tratruoc
			, heso_khuyenkhich, heso_kvdacthu, heso_vtcv_nvptm, heso_vtcv_dai, heso_vtcv_nvhotro
			, heso_hotro_nvptm, heso_hotro_dai, heso_hotro_nvhotro, heso_quydinh_nvptm, heso_quydinh_dai
			, heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_hoso, heso_daily, dongia, doanhthu_dongia_nvptm, heso_dichvu_dnhm, diaban, ma_duan_banhang, manv_hotro, doituong_kh, kiemtra_duan, ma_da
			, mst, tyle_am, tyle_hotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, doanhthu_dongia_nvhotro, doanhthu_kpi_nvhotro, doanhthu_dongia_dnhm
			, loaihd_id, ghi_chu, vanban_id
--		, thuebao_id, hdtb_id, ungdung_id, chuquan_id, tinh_id
		--, b.*
			from ttkd_bsc.ct_bsc_ptm a
--						left join (select hdtb_id, dthu_Goi_Old, Dthu_Goi_New from ttkd_bct.thaydoitocdo_202401) b
--											on a.hdtb_id=b.hdtb_id
--								  update ttkd_bsc.ct_bsc_ptm set LYDO_KHONGTINH_LUONG = null, LYDO_KHONGTINH_DONGIA = null, TRANGTHAI_TT_ID = 1
--																							,  THANG_TLDG_DT = 202406, THANG_TLKPI = 202406, THANG_TLKPI_TO = 202406, THANG_TLKPI_PHONG = 202406
--																							, THANG_TLDG_DT_NVHOTRO = 202406, THANG_TLKPI_HOTRO = 202406
																							
						--		select * from ttkd_bsc.ct_bsc_ptm 
								where  thang_ptm >= 202405
								and (--ma_duan_banhang in ('237660')  or
--											ma_duan_banhang in ('206876', '163041')  or
											 ma_tb = 'hcm_vnpt_his_00000078'
--											 or ma_tb='hcm_ca_00053232'
--										or ma_gd in ('HCM-LD/01589291')
										)
;

---check bang db
	select * from css_hcm.hd_khachhang where ma_gd in ('HCM-LD/01640461');
	select * from css_hcm.db_thuebao where ma_tb = 'hcm_connect_00000100'
	;
---
commit;
;
select goi_cuoc, thang_luong, thang_ptm, MA_NGUOIGT, NGUOI_GT, manv_ptm, tennv_ptm, ma_gd, dich_vu, dichvuvt_id, loaitb_id, ma_tb, sothang_dc, LYDO_KHONGTINH_luong, LYDO_KHONGTINH_DONGIA
			, luong_dongia_nvptm, luong_dongia_nvhotro, nguon, nop_du, mien_hsgoc, trangthai_tt_id, trangthaitb_id, trangthaitb_id_n1
			, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong
			, ngay_luuhs_ttkd, ngay_luuhs_ttvt
		, thang_bddc, dthu_ps_truoc, dthu_ps, dthu_goi, tyle_huongdt, heso_dichvu, heso_dichvu_1, phanloai_kh, heso_khachhang, heso_tbnganhan, heso_tratruoc
		, heso_khuyenkhich, heso_kvdacthu, heso_vtcv_nvptm, heso_vtcv_dai, heso_vtcv_nvhotro
		, heso_hotro_nvptm, heso_hotro_dai, heso_hotro_nvhotro, heso_quydinh_nvptm, heso_quydinh_dai
		, heso_quydinh_nvhotro, heso_diaban_tinhkhac, heso_hoso, heso_daily, dongia, doanhthu_dongia_nvptm, heso_dichvu_dnhm, diaban, ma_duan_banhang, manv_hotro, doituong_kh, kiemtra_duan, ma_da
		, mst, tyle_am, tyle_hotro, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, doanhthu_dongia_nvhotro, doanhthu_kpi_nvhotro
		, loaihd_id, ghi_chu
--		, thuebao_id, hdtb_id, ungdung_id, chuquan_id, tinh_id

			from ttkd_bsc.ct_bsc_ptm a
								where  thang_ptm >= 202402 and thang_luong < 100
											and nvl(dthu_goi, 0) > 0
          

;
update ttkd_bsc.ct_bsc_ptm 
		set THANG_TLDG_DT = 0, THANG_TLKPI = 0, THANG_TLKPI_TO = 0, THANG_TLKPI_phong = 0, thang_tldg_dt_nvhotro = 0, thang_tlkpi_hotro = 0
				, thang_luong = 103, ghi_chu = ghi_chu || '; a Nghia yc xet duyet ngoai ctr, vi dthu _quay long thiet bi'
where id in (8775244, 8775828, 8776290, 8775445)
;

commit;