import { Location } from '../types';
import { useRouter } from 'next/navigation';

export async function handleAddressSubmit(
  address: string,
  router: ReturnType<typeof useRouter>,
): Promise<void> {
  try {
    // Use NEXT_PUBLIC_API_URL from environment variables
    const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/geocode`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ address }),
    });

    if (!response.ok) {
      throw new Error('Network response was not ok');
    }

    const data = await response.json();
    const location: Location = {
      latitude: data.latitude,
      longitude: data.longitude,
    };
    console.log('Address geocoded:', location);
    router.push(
      `/search?latitude=${location.latitude}&longitude=${location.longitude}`,
    );
  } catch (error) {
    console.error('Error geocoding address:', error);
  }
}
