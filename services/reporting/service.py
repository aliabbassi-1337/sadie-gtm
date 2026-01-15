"""Reporting service for generating and uploading Excel reports."""

import os
import tempfile
from abc import ABC, abstractmethod
from typing import List, Optional

from loguru import logger
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

from services.reporting import repo
from db.models.reporting import HotelLead, CityStats, EngineCount, ReportStats
from infra.s3 import upload_file


class IService(ABC):
    """Reporting Service - Generate and deliver reports to stakeholders."""

    @abstractmethod
    async def export_city(self, city: str, state: str, country: str = "USA") -> str:
        """Generate Excel report for a city and upload to S3.

        Returns S3 URI of uploaded report.
        """
        pass

    @abstractmethod
    async def export_state(self, state: str, country: str = "USA") -> str:
        """Generate Excel report for an entire state and upload to S3.

        Returns S3 URI of uploaded report.
        """
        pass

    @abstractmethod
    async def export_state_with_cities(self, state: str, country: str = "USA") -> List[str]:
        """Export all cities in a state plus state aggregate.

        Returns list of S3 URIs for all uploaded reports.
        """
        pass


class Service(IService):
    """Implementation of the reporting service."""

    def __init__(self) -> None:
        pass

    async def export_city(self, city: str, state: str, country: str = "USA") -> str:
        """Generate Excel report for a city and upload to S3."""
        logger.info(f"Generating report for {city}, {state}")

        # Get data from database
        leads = await repo.get_leads_for_city(city, state)
        stats = await repo.get_city_stats(city, state)
        top_engines = await repo.get_top_engines_for_city(city, state)

        report_stats = ReportStats(
            location_name=city,
            stats=stats,
            top_engines=top_engines,
        )

        # Generate Excel workbook
        workbook = self._create_workbook(leads, report_stats)

        # Save to temp file and upload
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            workbook.save(tmp.name)
            tmp_path = tmp.name

        try:
            # S3 path: HotelLeadGen/{country}/{state}/{city}.xlsx
            s3_key = f"HotelLeadGen/{country}/{state}/{city}.xlsx"
            s3_uri = upload_file(tmp_path, s3_key)
            logger.info(f"Uploaded city report to {s3_uri}")
            return s3_uri
        finally:
            os.unlink(tmp_path)

    async def export_state(self, state: str, country: str = "USA") -> str:
        """Generate Excel report for an entire state and upload to S3."""
        logger.info(f"Generating state aggregate report for {state}")

        # Get data from database
        leads = await repo.get_leads_for_state(state)
        stats = await repo.get_state_stats(state)
        top_engines = await repo.get_top_engines_for_state(state)

        report_stats = ReportStats(
            location_name=state,
            stats=stats,
            top_engines=top_engines,
        )

        # Generate Excel workbook
        workbook = self._create_workbook(leads, report_stats)

        # Save to temp file and upload
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            workbook.save(tmp.name)
            tmp_path = tmp.name

        try:
            # S3 path: HotelLeadGen/{country}/{state}/{state}.xlsx (state aggregate)
            s3_key = f"HotelLeadGen/{country}/{state}/{state}.xlsx"
            s3_uri = upload_file(tmp_path, s3_key)
            logger.info(f"Uploaded state aggregate report to {s3_uri}")
            return s3_uri
        finally:
            os.unlink(tmp_path)

    async def export_state_with_cities(self, state: str, country: str = "USA") -> List[str]:
        """Export all cities in a state plus state aggregate."""
        logger.info(f"Exporting all cities in {state}")

        uploaded_uris = []

        # Get all cities in state
        cities = await repo.get_cities_in_state(state)
        logger.info(f"Found {len(cities)} cities in {state}")

        # Export each city
        for city in cities:
            try:
                uri = await self.export_city(city, state, country)
                uploaded_uris.append(uri)
            except Exception as e:
                logger.error(f"Failed to export {city}, {state}: {e}")
                continue

        # Export state aggregate
        try:
            uri = await self.export_state(state, country)
            uploaded_uris.append(uri)
        except Exception as e:
            logger.error(f"Failed to export state aggregate for {state}: {e}")

        logger.info(f"Exported {len(uploaded_uris)} reports for {state}")
        return uploaded_uris

    def _create_workbook(
        self,
        leads: List[HotelLead],
        report_stats: ReportStats,
    ) -> Workbook:
        """Create Excel workbook with leads and stats tabs."""
        workbook = Workbook()

        # Create leads sheet (first tab)
        leads_sheet = workbook.active
        leads_sheet.title = "Leads"
        self._populate_leads_sheet(leads_sheet, leads)

        # Create stats sheet (second tab)
        stats_sheet = workbook.create_sheet(title="Stats")
        self._populate_stats_sheet(stats_sheet, report_stats)

        return workbook

    def _populate_leads_sheet(self, sheet, leads: List[HotelLead]) -> None:
        """Populate the leads sheet with hotel data."""
        # Define headers
        headers = ["Hotel", "Website", "Booking Engine", "Room Count", "Proximity"]

        # Style definitions
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        header_alignment = Alignment(horizontal="center", vertical="center")
        thin_border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="thin"),
        )

        # Write headers
        for col, header in enumerate(headers, 1):
            cell = sheet.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
            cell.border = thin_border

        # Write data rows
        for row, lead in enumerate(leads, 2):
            # Hotel name
            sheet.cell(row=row, column=1, value=lead.hotel_name).border = thin_border

            # Website
            website = lead.website or ""
            sheet.cell(row=row, column=2, value=website).border = thin_border

            # Booking engine (just the name, no domain)
            engine_name = lead.booking_engine_name or ""
            sheet.cell(row=row, column=3, value=engine_name).border = thin_border

            # Room count
            room_count = lead.room_count if lead.room_count else ""
            sheet.cell(row=row, column=4, value=room_count).border = thin_border

            # Proximity: "Nearest: Hotel Name (X.Xkm)"
            proximity = self._format_proximity(lead)
            sheet.cell(row=row, column=5, value=proximity).border = thin_border

        # Auto-adjust column widths
        for col in range(1, len(headers) + 1):
            max_length = len(headers[col - 1])
            for row in range(2, len(leads) + 2):
                cell_value = sheet.cell(row=row, column=col).value
                if cell_value:
                    max_length = max(max_length, len(str(cell_value)))
            sheet.column_dimensions[get_column_letter(col)].width = min(max_length + 2, 60)

    def _format_proximity(self, lead: HotelLead) -> str:
        """Format proximity string for a lead."""
        if not lead.nearest_customer_name or lead.nearest_customer_distance_km is None:
            return ""

        distance = float(lead.nearest_customer_distance_km)
        return f"Nearest: {lead.nearest_customer_name} ({distance:.1f}km)"

    def _populate_stats_sheet(self, sheet, report_stats: ReportStats) -> None:
        """Populate the stats sheet with analytics dashboard."""
        stats = report_stats.stats
        location = report_stats.location_name.upper()

        # Style definitions
        title_font = Font(bold=True, size=14)
        section_font = Font(bold=True, size=11)
        header_fill = PatternFill(start_color="D9E2F3", end_color="D9E2F3", fill_type="solid")

        # Title
        sheet.cell(row=1, column=1, value=f"LEAD GENERATION DASHBOARD - {location}")
        sheet.cell(row=1, column=1).font = title_font
        sheet.merge_cells("A1:F1")

        # FUNNEL Section (left side)
        sheet.cell(row=3, column=1, value="FUNNEL")
        sheet.cell(row=3, column=1).font = section_font

        # Calculate percentages
        with_website_pct = (
            (stats.with_website / stats.total_scraped * 100)
            if stats.total_scraped > 0
            else 0
        )
        booking_found_pct = (
            (stats.booking_found / stats.with_website * 100)
            if stats.with_website > 0
            else 0
        )

        # Funnel data
        sheet.cell(row=4, column=1, value="Hotels Scraped")
        sheet.cell(row=4, column=2, value=stats.total_scraped)

        sheet.cell(row=5, column=1, value="With Website")
        sheet.cell(row=5, column=2, value=f"{stats.with_website} ({with_website_pct:.1f}%)")

        sheet.cell(row=6, column=1, value="Booking Found")
        sheet.cell(row=6, column=2, value=f"{stats.booking_found} ({booking_found_pct:.1f}%)")

        # LEAD QUALITY Section (right side)
        sheet.cell(row=3, column=4, value="LEAD QUALITY (of Booking Found)")
        sheet.cell(row=3, column=4).font = section_font

        total_with_booking = stats.tier_1_count + stats.tier_2_count
        tier_1_pct = (
            (stats.tier_1_count / total_with_booking * 100)
            if total_with_booking > 0
            else 0
        )
        tier_2_pct = (
            (stats.tier_2_count / total_with_booking * 100)
            if total_with_booking > 0
            else 0
        )

        sheet.cell(row=4, column=4, value="Tier 1 (Known Engine)")
        sheet.cell(row=4, column=5, value=f"{stats.tier_1_count} ({tier_1_pct:.1f}%)")

        sheet.cell(row=5, column=4, value="Tier 2 (Unknown Engine)")
        sheet.cell(row=5, column=5, value=f"{stats.tier_2_count} ({tier_2_pct:.1f}%)")

        sheet.cell(row=6, column=4, value="Total")
        sheet.cell(row=6, column=5, value=f"{total_with_booking} (100%)")

        # CONTACT INFO Section
        sheet.cell(row=8, column=1, value="CONTACT INFO")
        sheet.cell(row=8, column=1).font = section_font

        with_phone_pct = (
            (stats.with_phone / stats.total_scraped * 100)
            if stats.total_scraped > 0
            else 0
        )
        with_email_pct = (
            (stats.with_email / stats.total_scraped * 100)
            if stats.total_scraped > 0
            else 0
        )

        sheet.cell(row=9, column=1, value="With Phone")
        sheet.cell(row=9, column=2, value=f"{stats.with_phone} ({with_phone_pct:.1f}%)")

        sheet.cell(row=10, column=1, value="With Email")
        sheet.cell(row=10, column=2, value=f"{stats.with_email} ({with_email_pct:.1f}%)")

        # TOP ENGINES Section (right side)
        sheet.cell(row=8, column=4, value="TOP ENGINES")
        sheet.cell(row=8, column=4).font = section_font

        for idx, engine in enumerate(report_stats.top_engines):
            row = 9 + idx
            sheet.cell(row=row, column=4, value=engine.engine_name)
            sheet.cell(row=row, column=5, value=engine.hotel_count)

        # Auto-adjust column widths
        sheet.column_dimensions["A"].width = 20
        sheet.column_dimensions["B"].width = 18
        sheet.column_dimensions["C"].width = 5
        sheet.column_dimensions["D"].width = 28
        sheet.column_dimensions["E"].width = 15
